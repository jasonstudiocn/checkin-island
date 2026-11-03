/* 打卡岛 · 云端同步端点
 *   方案 3b：Vercel 函数 + Turso(libSQL)
 *
 * 零依赖：不引任何 SDK，直接用 fetch 走 Turso 的 HTTP pipeline 协议（Hrana v2）。
 *
 * 环境变量（在 Vercel 项目里设置）：
 *   TURSO_URL    形如 libsql://xxxx.turso.io 或 https://xxxx.turso.io
 *   TURSO_TOKEN  Turso 数据库的访问令牌
 *
 * 安全模型：
 *   「同步码」既当钥匙也当身份，只存在用户设备本机，从不随数据同步。
 *   库里只存它的 SHA-256，原文不落库（按哈希主键查询，本身就是一次定长比对）。
 *
 * 接口（全部 POST /api/sync，JSON）：
 *   { op:'ping' }                                    探活
 *   { op:'pull', key }                               读取
 *   { op:'push', key, baseVer, payload }             写入（乐观并发）
 *   { op:'delete', key }                             删除云端副本
 */
'use strict';

var crypto = require('crypto');

var TABLE = 'sync';
var MAX_PAYLOAD = 200000;                      // 约 200KB，正常存档 2~5KB
var KEY_RE = /^ck_[a-z0-9]{16,64}$/;
var TIMEOUT_MS = 8000;
var RL_WINDOW_MS = 5 * 60 * 1000;
var RL_MAX = 150;                              // 每个同步码 5 分钟内最多 150 次

var _hits = new Map();

function rateLimited(id) {
  var now = Date.now();
  var rec = _hits.get(id);
  if (!rec || now - rec.t > RL_WINDOW_MS) { rec = { t: now, n: 0 }; _hits.set(id, rec); }
  rec.n++;
  if (_hits.size > 5000) {                     // 防止内存无限增长
    var cut = now - RL_WINDOW_MS;
    _hits.forEach(function (v, k) { if (v.t < cut) _hits.delete(k); });
  }
  return rec.n > RL_MAX;
}

/* ---------------- Turso（libSQL over HTTP） ---------------- */

// 兼容两种命名：自己手填的 TURSO_URL/TURSO_TOKEN，
// 以及 Turso 官方 Vercel 集成自动注入的 TURSO_DATABASE_URL/TURSO_AUTH_TOKEN
function envUrl() { return String(process.env.TURSO_URL || process.env.TURSO_DATABASE_URL || '').trim(); }
function envToken() { return String(process.env.TURSO_TOKEN || process.env.TURSO_AUTH_TOKEN || '').trim(); }

function tursoEndpoint() {
  var u = envUrl();
  if (!u) return '';
  var local = /^https?:\/\/(127\.0\.0\.1|localhost)(:\d+)?(\/|$)/.test(u);   // 本地调试用
  if (!local) {
    u = u.replace(/^libsql:\/\//, 'https://').replace(/^http:\/\//, 'https://');
  }
  if (!/^https?:\/\//.test(u)) u = 'https://' + u;
  return u.replace(/\/+$/, '') + '/v2/pipeline';
}

function configured() {
  return !!(envUrl() && envToken());
}

function toVal(v) {
  if (v === null || v === undefined) return { type: 'null' };
  if (typeof v === 'number') {
    return Number.isInteger(v) ? { type: 'integer', value: String(v) } : { type: 'float', value: v };
  }
  if (typeof v === 'boolean') return { type: 'integer', value: v ? '1' : '0' };
  return { type: 'text', value: String(v) };
}

function fromVal(c) {
  if (!c || c.type === 'null' || c.type === 'blob') return c && c.type === 'blob' ? null : null;
  if (c.type === 'integer') return Number(c.value);
  if (c.type === 'float') return Number(c.value);
  return c.value;
}

function rowObj(res) {
  if (!res) return null;
  var cols = res.cols || [];
  var row = (res.rows && res.rows[0]) || null;
  if (!row) return null;
  var o = {};
  for (var i = 0; i < cols.length; i++) o[cols[i]] = row[i];
  return o;
}

// 一次 HTTP 往返执行多条语句，返回每条的 { cols, rows, affected }
async function sql(stmts) {
  var url = tursoEndpoint();
  var requests = stmts.map(function (s) {
    return { type: 'execute', stmt: { sql: s.sql, args: (s.args || []).map(toVal) } };
  });
  requests.push({ type: 'close' });

  var ctrl = typeof AbortController === 'function' ? new AbortController() : null;
  var timer = ctrl ? setTimeout(function () { ctrl.abort(); }, TIMEOUT_MS) : null;
  var res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + envToken()
      },
      body: JSON.stringify({ requests: requests }),
      signal: ctrl ? ctrl.signal : undefined
    });
  } catch (e) {
    throw new Error('连不上云端数据库' + (e && e.name === 'AbortError' ? '（超时）' : ''));
  } finally {
    if (timer) clearTimeout(timer);
  }

  if (!res.ok) throw new Error('云端数据库返回 HTTP ' + res.status);
  var json = null;
  try { json = await res.json(); } catch (e) { json = null; }
  if (!json || !Array.isArray(json.results)) throw new Error('云端返回格式不对');

  var out = [];
  for (var i = 0; i < json.results.length; i++) {
    var r = json.results[i];
    if (r && r.type === 'error') {
      throw new Error('云端 SQL 出错：' + ((r.error && r.error.message) || '未知原因'));
    }
    var resp = (r && r.response) || {};
    if (resp.type === 'execute') {
      var result = resp.result || {};
      out.push({
        cols: (result.cols || []).map(function (c) { return c.name; }),
        rows: (result.rows || []).map(function (row) { return row.map(fromVal); }),
        affected: result.affected_row_count || 0
      });
    } else {
      out.push(null);
    }
  }
  return out;
}

var CREATE_SQL =
  'CREATE TABLE IF NOT EXISTS ' + TABLE +
  ' (k TEXT PRIMARY KEY, payload TEXT NOT NULL, ver INTEGER NOT NULL, updated_at INTEGER NOT NULL)';

/* ---------------- HTTP 辅助 ---------------- */

function send(res, code, obj) {
  res.statusCode = code;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify(obj));
}

function fail(res, code, err, message) {
  send(res, code, { ok: false, error: err, message: message || '' });
}

function ipOf(req) {
  var h = req.headers || {};
  var xf = h['x-forwarded-for'] || h['x-real-ip'] || '';
  return String(xf).split(',')[0].trim() || 'unknown';
}

/* ---------------- 主流程 ---------------- */

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Max-Age', '86400');

  if (req.method === 'OPTIONS') { res.statusCode = 204; res.end(); return; }
  if (req.method !== 'POST') return fail(res, 405, 'method_not_allowed', '只接受 POST');

  var body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = null; } }
  if (!body || typeof body !== 'object') return fail(res, 400, 'bad_body', '请求内容是空的或不是合法 JSON');

  var op = String(body.op || '');
  if (['ping', 'pull', 'push', 'delete'].indexOf(op) < 0) return fail(res, 400, 'bad_op', '不认识的操作：' + op);

  if (op === 'ping') {
    if (!configured()) return fail(res, 503, 'not_configured', '云端数据库还没配好');
    try {
      await sql([{ sql: 'SELECT 1' }]);
      return send(res, 200, { ok: true, ts: Date.now() });
    } catch (e) {
      return fail(res, 502, 'upstream', '云端数据库连不上：' + e.message);
    }
  }

  var key = String(body.key || '').trim().toLowerCase();
  if (!KEY_RE.test(key)) return fail(res, 400, 'bad_key', '同步码格式不对');
  var k = crypto.createHash('sha256').update(key, 'utf8').digest('hex');

  if (rateLimited(k)) return fail(res, 429, 'rate_limited', '请求太频繁了，歇一会儿再试');

  if (!configured()) return fail(res, 503, 'not_configured', '云端数据库还没配好');

  try {
    /* ---- 读取 ---- */
    if (op === 'pull') {
      var r1 = await sql([
        { sql: CREATE_SQL },
        { sql: 'SELECT payload, ver, updated_at FROM ' + TABLE + ' WHERE k = ?', args: [k] }
      ]);
      var row = rowObj(r1[1]);
      if (!row) return send(res, 200, { ok: true, empty: true, ver: 0 });
      return send(res, 200, {
        ok: true, empty: false,
        ver: Number(row.ver) || 1,
        updatedAt: Number(row.updated_at) || 0,
        payload: String(row.payload || '')
      });
    }

    /* ---- 删除 ---- */
    if (op === 'delete') {
      await sql([
        { sql: CREATE_SQL },
        { sql: 'DELETE FROM ' + TABLE + ' WHERE k = ?', args: [k] }
      ]);
      return send(res, 200, { ok: true, deleted: true });
    }

    /* ---- 写入（乐观并发：baseVer 对不上就返回云端那一份，让客户端合并后重试） ---- */
    var payload = body.payload;
    if (typeof payload !== 'string' || !payload) return fail(res, 400, 'bad_payload', '数据是空的');
    if (payload.length > MAX_PAYLOAD) return fail(res, 413, 'too_large', '数据太大了（超过 200KB）');
    var parsed = null;
    try { parsed = JSON.parse(payload); } catch (e) { parsed = null; }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return fail(res, 400, 'bad_payload', '数据不是合法的 JSON 对象');
    }

    var baseVer = Number(body.baseVer);
    if (!Number.isFinite(baseVer) || baseVer < 0) baseVer = 0;
    baseVer = Math.floor(baseVer);
    var now = Date.now();

    var write = baseVer === 0
      ? { sql: 'INSERT OR IGNORE INTO ' + TABLE + ' (k, payload, ver, updated_at) VALUES (?,?,?,?)',
          args: [k, payload, 1, now] }
      : { sql: 'UPDATE ' + TABLE + ' SET payload = ?, ver = ?, updated_at = ? WHERE k = ? AND ver = ?',
          args: [payload, baseVer + 1, now, k, baseVer] };

    var r2 = await sql([
      { sql: CREATE_SQL },
      write,
      { sql: 'SELECT payload, ver, updated_at FROM ' + TABLE + ' WHERE k = ?', args: [k] }
    ]);

    if (r2[1] && r2[1].affected > 0) {
      var cur = rowObj(r2[2]);
      return send(res, 200, { ok: true, ver: cur ? Number(cur.ver) : (baseVer || 1), updatedAt: now });
    }

    var existing = rowObj(r2[2]);
    if (existing) {
      // 别的设备在我们读完之后写过 —— 把云端那份交回去，客户端合并后重推
      return send(res, 409, {
        ok: false, conflict: true,
        ver: Number(existing.ver) || 1,
        updatedAt: Number(existing.updated_at) || 0,
        payload: String(existing.payload || ''),
        message: '云端被其他设备改过'
      });
    }

    // 云端根本没有这条记录（比如被删过），就当新建
    var r3 = await sql([
      { sql: CREATE_SQL },
      { sql: 'INSERT OR IGNORE INTO ' + TABLE + ' (k, payload, ver, updated_at) VALUES (?,?,?,?)', args: [k, payload, 1, now] },
      { sql: 'SELECT ver FROM ' + TABLE + ' WHERE k = ?', args: [k] }
    ]);
    var again = rowObj(r3[2]);
    if (!again) throw new Error('写入没有生效');
    return send(res, 200, { ok: true, ver: Number(again.ver) || 1, updatedAt: now });

  } catch (e) {
    return fail(res, 502, 'upstream', '云端数据库暂时不可用：' + ((e && e.message) || '未知原因'));
  }
};
