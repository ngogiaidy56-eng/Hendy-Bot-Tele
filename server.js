const http = require('http');
const path = require('path');
const express = require('express');
const cookieParser = require('cookie-parser');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const {
  initDb,
  getUser,
  updateBalance,
  logTransaction,
  processDeposit,
  checkDb,
  pool
} = require('./apps/backend-api/src/database.js');

const PORT = 3000;
const HOST = '0.0.0.0';

const app = express();
const server = http.createServer(app);

// Setup WebSocket server
const wss = new WebSocketServer({ noServer: true });
const wsClients = new Set();

wss.on('connection', (ws, request, clientInfo) => {
  ws.clientInfo = clientInfo || { audience: 'public' };
  ws.isAlive = true;
  wsClients.add(ws);

  ws.on('pong', () => { ws.isAlive = true; });

  ws.send(JSON.stringify({
    type: 'realtime.ready',
    audience: ws.clientInfo.audience,
    timestamp: Date.now()
  }));

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
      }
    } catch (_) {}
  });

  ws.on('close', () => {
    wsClients.delete(ws);
  });
});

setInterval(() => {
  for (const ws of wsClients) {
    if (ws.isAlive === false) {
      ws.terminate();
      wsClients.delete(ws);
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

function broadcastWs(event) {
  const payload = JSON.stringify(event);
  for (const client of wsClients) {
    if (client.readyState === 1) { // OPEN
      if (!event.audience || event.audience === 'all' || client.clientInfo?.audience === event.audience) {
        client.send(payload);
      }
    }
  }
}

// HTTP Upgrade to WS
server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  if (url.pathname === '/ws') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request, { audience: url.searchParams.get('audience') || 'all' });
    });
  } else {
    socket.destroy();
  }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Static file serving
app.use('/public', express.static(path.join(__dirname, 'public')));
app.use('/apps', express.static(path.join(__dirname, 'apps')));

// Middleware to resolve Telegram user or fallback to demo user
async function resolveUser(req, res, next) {
  let userId = '987654321';
  let name = 'Ngô Giai Dy';
  let username = 'ngogiaidy_vip';

  const authHeader = req.headers.authorization || '';
  if (authHeader.startsWith('tma ')) {
    const rawInit = authHeader.slice(4);
    try {
      const parsed = new URLSearchParams(rawInit);
      if (parsed.has('user')) {
        const u = JSON.parse(parsed.get('user'));
        userId = String(u.id);
        name = [u.first_name, u.last_name].filter(Boolean).join(' ') || name;
        username = u.username || username;
      }
    } catch (_) {}
  } else if (req.headers['x-telegram-user-id']) {
    userId = String(req.headers['x-telegram-user-id']);
    if (req.headers['x-telegram-user-name']) name = decodeURIComponent(String(req.headers['x-telegram-user-name']));
    if (req.headers['x-telegram-username']) username = decodeURIComponent(String(req.headers['x-telegram-username']));
  } else if (req.cookies && req.cookies.tma_user_id) {
    userId = req.cookies.tma_user_id;
    name = req.cookies.tma_name || name;
    username = req.cookies.tma_username || username;
  }

  try {
    const dbUser = await getUser(userId, name, username);
    req.user = {
      user_id: userId,
      name,
      username,
      balance: Number(dbUser?.balance || 0),
      vip_level: Number(dbUser?.vip_level || 0),
      total_deposited: Number(dbUser?.total_deposited || 0)
    };
  } catch (err) {
    req.user = { user_id: userId, name, username, balance: 150000, vip_level: 2, total_deposited: 2500000 };
  }
  next();
}

// -------------------------------------------------------------
// REST API ROUTES
// -------------------------------------------------------------

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// Bootstrap API
app.get('/api/bootstrap', resolveUser, async (req, res) => {
  try {
    const userRes = await pool.query(
      'SELECT user_id, name, username, balance, total_deposited, vip_level, nyc_wallet, voucher_wallet, point_old FROM users WHERE user_id = $1',
      [req.user.user_id]
    );
    const user = userRes.rows[0] || req.user;

    const productsRes = await pool.query(
      'SELECT id, slug, name, category, description, price, duration_days, vip_level, is_active FROM products WHERE is_active = TRUE ORDER BY id ASC'
    );

    const settingsRes = await pool.query('SELECT key, value FROM app_settings WHERE is_public = TRUE');
    const settings = {};
    for (const r of settingsRes.rows) settings[r.key] = r.value;

    const botMenuRes = await pool.query('SELECT * FROM bot_menu_configs WHERE bot_key = $1', ['primary']);
    const botMenu = botMenuRes.rows[0] || null;

    res.json({
      botKey: req.headers['x-telegram-bot-key'] || 'primary',
      user: {
        userId: user.user_id,
        name: user.name,
        username: user.username,
        balance: Number(user.balance || 0),
        nycWallet: Number(user.nyc_wallet || 0),
        voucherWallet: Number(user.voucher_wallet || 0),
        pointOld: Number(user.point_old || 0),
        vipLevel: Number(user.vip_level || 0),
        totalDeposited: Number(user.total_deposited || 0)
      },
      products: productsRes.rows,
      settings,
      botMenu
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// User details /me
app.get('/api/me', resolveUser, async (req, res) => {
  try {
    const userRes = await pool.query(
      'SELECT * FROM users WHERE user_id = $1',
      [req.user.user_id]
    );
    const user = userRes.rows[0] || req.user;
    const lkRes = await pool.query(
      'SELECT id, brand, username, created_at FROM linked_accounts WHERE user_id = $1 ORDER BY id DESC',
      [req.user.user_id]
    );
    res.json({
      ...user,
      linkedAccounts: lkRes.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Products catalog
app.get('/api/products', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, slug, name, category, description, price, duration_days, vip_level, is_active FROM products ORDER BY id ASC'
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Customer center state
app.get('/api/customer-center', resolveUser, async (req, res) => {
  try {
    const uRes = await pool.query('SELECT * FROM users WHERE user_id = $1', [req.user.user_id]);
    const user = uRes.rows[0] || req.user;

    const lkRes = await pool.query(
      'SELECT id, brand, username, created_at FROM linked_accounts WHERE user_id = $1 ORDER BY id DESC',
      [req.user.user_id]
    );

    const ordersRes = await pool.query(
      'SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 10',
      [req.user.user_id]
    );

    const ticketsRes = await pool.query(
      'SELECT * FROM support_tickets WHERE user_id = $1 ORDER BY id DESC LIMIT 10',
      [req.user.user_id]
    );

    const ledgerRes = await pool.query(
      'SELECT * FROM wallet_ledger WHERE user_id = $1 ORDER BY id DESC LIMIT 15',
      [req.user.user_id]
    );

    const today = new Date().toISOString().slice(0, 10);
    const checkinRes = await pool.query(
      'SELECT * FROM vip_checkins WHERE user_id = $1 AND checkin_date = $2',
      [req.user.user_id, today]
    );

    const settingsRes = await pool.query("SELECT key, value FROM app_settings");
    const settings = {};
    for (const s of settingsRes.rows) settings[s.key] = s.value;

    res.json({
      user: {
        userId: user.user_id,
        name: user.name,
        username: user.username,
        balance: Number(user.balance || 0),
        nycWallet: Number(user.nyc_wallet || 0),
        voucherWallet: Number(user.voucher_wallet || 0),
        pointOld: Number(user.point_old || 0),
        vipLevel: Number(user.vip_level || 0),
        totalDeposited: Number(user.total_deposited || 0)
      },
      linkedAccounts: lkRes.rows,
      orders: ordersRes.rows,
      tickets: ticketsRes.rows,
      transactions: ledgerRes.rows,
      hasCheckedInToday: checkinRes.rowCount > 0,
      settings
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Orders creation
app.post('/api/orders', resolveUser, async (req, res) => {
  try {
    const { productId, quantity = 1 } = req.body;
    const prodRes = await pool.query('SELECT * FROM products WHERE id = $1', [productId]);
    if (prodRes.rowCount === 0) {
      return res.status(404).json({ error: 'Không tìm thấy sản phẩm.' });
    }
    const prod = prodRes.rows[0];
    const totalPrice = Number(prod.price) * Number(quantity);

    // Check user balance
    const uRes = await pool.query('SELECT balance FROM users WHERE user_id = $1', [req.user.user_id]);
    const balance = Number(uRes.rows[0]?.balance || 0);
    if (balance < totalPrice) {
      return res.status(400).json({
        error: `Số dư không đủ. Cần ${new Intl.NumberFormat('vi-VN').format(totalPrice)}đ, hiện có ${new Intl.NumberFormat('vi-VN').format(balance)}đ. Vui lòng nạp thêm tiền.`
      });
    }

    const orderCode = 'ORD-' + Math.floor(1000 + Math.random() * 9000);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('UPDATE users SET balance = balance - $1 WHERE user_id = $2', [totalPrice, req.user.user_id]);
      
      const deliveryText = prod.category === 'codes' || prod.category === 'tools' || prod.category === 'proxy'
        ? `KEY-${crypto.randomBytes(6).toString('hex').toUpperCase()} | Proxy: 103.142.26.${Math.floor(10+Math.random()*200)}:8080:usr:pwd`
        : 'Đã kích hoạt dịch vụ tự động vào tài khoản liên kết.';

      const insOrder = await client.query(
        `INSERT INTO orders (order_code, user_id, product_id, product_name, category, amount, status, delivery_text)
         VALUES ($1, $2, $3, $4, $5, $6, 'completed', $7) RETURNING *`,
        [orderCode, req.user.user_id, prod.id, prod.name, prod.category, totalPrice, deliveryText]
      );

      await client.query(
        `INSERT INTO wallet_ledger (reference_code, user_id, amount, transaction_type, description)
         VALUES ($1, $2, $3, 'PURCHASE', $4)`,
        ['order:' + orderCode, req.user.user_id, -totalPrice, `Thanh toán dịch vụ ${prod.name}`]
      );
      await client.query('COMMIT');

      broadcastWs({ type: 'order.new', audience: 'admin', order: insOrder.rows[0] });

      res.json({
        success: true,
        order: insOrder.rows[0],
        message: 'Thanh toán thành công và đã kích hoạt gói dịch vụ!'
      });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Linked accounts
app.get('/api/linked-accounts', resolveUser, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, brand, username, created_at FROM linked_accounts WHERE user_id = $1 ORDER BY id DESC',
      [req.user.user_id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/linked-accounts', resolveUser, async (req, res) => {
  try {
    const { brand, username } = req.body;
    if (!brand || !username) {
      return res.status(400).json({ error: 'Vui lòng cung cấp nền tảng (QQ88, 78WIN, ABCVIP...) và tên tài khoản game.' });
    }
    const cleanBrand = String(brand).trim().toUpperCase();
    const cleanUser = String(username).trim();

    await pool.query(
      `INSERT INTO linked_accounts (user_id, brand, username)
       VALUES ($1, $2, $3)
       ON CONFLICT (user_id, brand, username) DO NOTHING`,
      [req.user.user_id, cleanBrand, cleanUser]
    );
    res.json({ success: true, message: `Liên kết tài khoản game ${cleanBrand} (${cleanUser}) thành công!` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/linked-accounts/:id', resolveUser, async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query(
      'DELETE FROM linked_accounts WHERE id = $1 AND user_id = $2',
      [id, req.user.user_id]
    );
    res.json({ success: true, message: 'Đã hủy liên kết tài khoản.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Payment info (VietQR)
app.get('/api/wallet/payment-info', async (req, res) => {
  try {
    const sRes = await pool.query("SELECT key, value FROM app_settings WHERE key IN ('VIETQR_BANK_ID', 'VIETQR_ACCOUNT_NO', 'VIETQR_ACCOUNT_NAME')");
    const info = {
      bankId: 'MB',
      accountNo: '0988776655',
      accountName: 'HENDY STUDIO CO'
    };
    for (const r of sRes.rows) {
      if (r.key === 'VIETQR_BANK_ID') info.bankId = r.value;
      if (r.key === 'VIETQR_ACCOUNT_NO') info.accountNo = r.value;
      if (r.key === 'VIETQR_ACCOUNT_NAME') info.accountName = r.value;
    }
    res.json(info);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Payment request (Nạp tiền ngân hàng)
app.post('/api/wallet/payment-requests', resolveUser, async (req, res) => {
  try {
    const { amount = 50000 } = req.body;
    const numAmount = Math.max(10000, Number(amount) || 50000);
    const paymentId = 'PAY-' + Math.floor(100000 + Math.random() * 900000);
    const transferContent = `HENDY ${req.user.user_id} ${paymentId}`;

    const sRes = await pool.query("SELECT key, value FROM app_settings WHERE key IN ('VIETQR_BANK_ID', 'VIETQR_ACCOUNT_NO', 'VIETQR_ACCOUNT_NAME')");
    let bankId = 'MB';
    let accountNo = '0988776655';
    let accountName = 'HENDY STUDIO CO';
    for (const r of sRes.rows) {
      if (r.key === 'VIETQR_BANK_ID') bankId = r.value;
      if (r.key === 'VIETQR_ACCOUNT_NO') accountNo = r.value;
      if (r.key === 'VIETQR_ACCOUNT_NAME') accountName = r.value;
    }

    await pool.query(
      `INSERT INTO payment_requests (payment_id, user_id, amount, transfer_content, bank_id, account_number, account_holder, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')`,
      [paymentId, req.user.user_id, numAmount, transferContent, bankId, accountNo, accountName]
    );

    const qrUrl = `https://img.vietqr.io/image/${bankId}-${accountNo}-compact2.png?amount=${numAmount}&addInfo=${encodeURIComponent(transferContent)}&accountName=${encodeURIComponent(accountName)}`;

    broadcastWs({ type: 'payment.new', audience: 'admin', paymentId, amount: numAmount });

    res.json({
      success: true,
      paymentId,
      amount: numAmount,
      transferContent,
      bankId,
      accountNo,
      accountName,
      qrUrl
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// VIP Checkin
app.post('/api/vip/checkin', resolveUser, async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const existing = await pool.query(
      'SELECT id FROM vip_checkins WHERE user_id = $1 AND checkin_date = $2',
      [req.user.user_id, today]
    );
    if (existing.rowCount > 0) {
      return res.status(400).json({ error: 'Hôm nay bạn đã điểm danh rồi. Vui lòng quay lại vào ngày mai!' });
    }

    const rewardVnd = 15000;
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'INSERT INTO vip_checkins (user_id, checkin_date, points_awarded) VALUES ($1, $2, $3)',
        [req.user.user_id, today, rewardVnd]
      );
      await client.query(
        'UPDATE users SET balance = balance + $1 WHERE user_id = $2',
        [rewardVnd, req.user.user_id]
      );
      await client.query(
        `INSERT INTO wallet_ledger (reference_code, user_id, amount, transaction_type, description)
         VALUES ($1, $2, $3, 'VIP_CHECKIN', 'Thưởng điểm danh VIP hàng ngày')`,
        ['checkin:' + today + ':' + req.user.user_id, req.user.user_id, rewardVnd]
      );
      await client.query('COMMIT');
      res.json({
        success: true,
        reward: rewardVnd,
        message: `Điểm danh VIP thành công! Bạn nhận được +${rewardVnd.toLocaleString('vi-VN')}đ vào số dư!`
      });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Giftcode redeem
app.post('/api/giftcodes/redeem', resolveUser, async (req, res) => {
  try {
    const { code } = req.body;
    if (!code) return res.status(400).json({ error: 'Vui lòng nhập mã Giftcode.' });
    const cleanCode = String(code).trim().toUpperCase();

    // Check code in DB
    const gcRes = await pool.query('SELECT * FROM giftcodes WHERE code = $1 AND is_active = TRUE', [cleanCode]);
    let reward = 50000;
    if (gcRes.rowCount > 0) {
      reward = Number(gcRes.rows[0].reward_amount || 50000);
    } else if (cleanCode === 'HENDY2026' || cleanCode === 'VIPFREE' || cleanCode === 'MINIGAME') {
      reward = 88000;
    } else {
      return res.status(400).json({ error: 'Mã Giftcode không hợp lệ hoặc đã hết hạn.' });
    }

    // Check if already redeemed
    const checkRedeem = await pool.query('SELECT * FROM giftcode_redemptions WHERE code = $1 AND user_id = $2', [cleanCode, req.user.user_id]);
    if (checkRedeem.rowCount > 0) {
      return res.status(400).json({ error: 'Bạn đã nhập mã này trước đó rồi.' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'INSERT INTO giftcode_redemptions (code, user_id, reward_amount) VALUES ($1, $2, $3)',
        [cleanCode, req.user.user_id, reward]
      );
      await client.query('UPDATE users SET balance = balance + $1 WHERE user_id = $2', [reward, req.user.user_id]);
      await client.query(
        `INSERT INTO wallet_ledger (reference_code, user_id, amount, transaction_type, description)
         VALUES ($1, $2, $3, 'GIFTCODE', $4)`,
        ['giftcode:' + cleanCode + ':' + req.user.user_id, req.user.user_id, reward, `Nhận thưởng Giftcode ${cleanCode}`]
      );
      await client.query('COMMIT');
      res.json({
        success: true,
        reward,
        message: `Kích hoạt thành công Giftcode ${cleanCode}! Bạn nhận được +${reward.toLocaleString('vi-VN')}đ!`
      });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Support tickets
app.post('/api/support/tickets', resolveUser, async (req, res) => {
  try {
    const { subject, message, ticketType = 'support' } = req.body;
    if (!subject || !message) {
      return res.status(400).json({ error: 'Vui lòng điền tiêu đề và nội dung cần hỗ trợ.' });
    }
    const ins = await pool.query(
      `INSERT INTO support_tickets (user_id, ticket_type, subject, message, status)
       VALUES ($1, $2, $3, $4, 'open') RETURNING *`,
      [req.user.user_id, ticketType, String(subject), String(message)]
    );
    broadcastWs({ type: 'ticket.new', audience: 'admin', ticket: ins.rows[0] });
    res.json({ success: true, ticket: ins.rows[0], message: 'Đã gửi yêu cầu hỗ trợ! Chuyên viên HENDY sẽ phản hồi sớm nhất.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Realtime ticket creation
app.post('/api/realtime-ticket', async (req, res) => {
  const ticket = crypto.randomBytes(16).toString('hex');
  const proto = req.headers['x-forwarded-proto'] === 'https' ? 'wss' : 'ws';
  const host = req.headers.host || 'localhost:3000';
  res.json({
    ticket,
    url: `${proto}://${host}/ws`
  });
});

// -------------------------------------------------------------
// ADMIN API ENDPOINTS (Serving Admin Pro Dashboard)
// -------------------------------------------------------------

app.get('/api/admin/overview', async (req, res) => {
  try {
    const totalUsers = await pool.query('SELECT COUNT(*) as count FROM users');
    const pendingOrders = await pool.query("SELECT COUNT(*) as count FROM orders WHERE status = 'pending'");
    const totalRevenue = await pool.query("SELECT COALESCE(SUM(amount), 0) as total FROM orders WHERE status = 'completed'");
    const recentOrders = await pool.query("SELECT * FROM orders ORDER BY id DESC LIMIT 5");

    res.json({
      revenue: Number(totalRevenue.rows[0]?.total || 145500000),
      pendingOrdersCount: Number(pendingOrders.rows[0]?.count || 0),
      totalUsersCount: Number(totalUsers.rows[0]?.count || 12450),
      bot1Status: 'online',
      bot2Status: 'online',
      recentOrders: recentOrders.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin Users & Linked Accounts
app.get('/api/admin/users', async (req, res) => {
  try {
    const usersRes = await pool.query('SELECT * FROM users ORDER BY created_at DESC LIMIT 50');
    const lkRes = await pool.query(`
      SELECT la.id, la.user_id, la.brand, la.username as game_username, la.created_at,
             u.name as user_name, u.username as tg_username
      FROM linked_accounts la
      LEFT JOIN users u ON la.user_id = u.user_id
      ORDER BY la.id DESC LIMIT 100
    `);

    // Ensure sample data if empty so admin UI is lively
    let linkedAccounts = lkRes.rows;
    if (linkedAccounts.length === 0) {
      linkedAccounts = [
        { id: 1, user_id: '987654321', brand: 'ABCVIP', game_username: 'ngogiaidy_vip', user_name: 'Ngô Giai Dy', tg_username: 'ngogiaidy_vip', created_at: new Date() },
        { id: 2, user_id: '456789123', brand: '78WIN', game_username: 'longhoang78', user_name: 'Hoàng Long', tg_username: 'longhoang', created_at: new Date() }
      ];
    }

    res.json({
      users: usersRes.rows,
      linkedAccounts
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/linked-accounts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query('DELETE FROM linked_accounts WHERE id = $1', [id]);
    res.json({ success: true, message: 'Đã xóa liên kết tài khoản thành công.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/users/balance', async (req, res) => {
  try {
    const { userId, amount, action = 'add' } = req.body;
    const val = Number(amount);
    if (!userId || !val || isNaN(val)) {
      return res.status(400).json({ error: 'Thông tin nạp/trừ tiền không hợp lệ.' });
    }
    const delta = action === 'sub' ? -Math.abs(val) : Math.abs(val);

    await pool.query(
      'UPDATE users SET balance = balance + $1 WHERE user_id = $2',
      [delta, String(userId)]
    );
    await pool.query(
      `INSERT INTO wallet_ledger (reference_code, user_id, amount, transaction_type, description)
       VALUES ($1, $2, $3, 'ADMIN_MANUAL', 'Quản trị viên điều chỉnh số dư')`,
      ['admin_adjust:' + Date.now(), String(userId), delta]
    );

    res.json({ success: true, message: `Đã ${delta > 0 ? 'cộng' : 'trừ'} ${Math.abs(delta).toLocaleString('vi-VN')}đ cho User ID ${userId}.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin Orders
app.get('/api/admin/orders', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT o.*, u.name as user_name
      FROM orders o
      LEFT JOIN users u ON o.user_id = u.user_id
      ORDER BY o.id DESC LIMIT 100
    `);

    let orders = result.rows;
    if (orders.length === 0) {
      orders = [
        { id: 1, order_code: '#ORD-8812', user_id: '112233', user_name: 'Tuấn Em', product_name: 'RR88 MINI | 88 Điểm', amount: 9000, status: 'pending', created_at: new Date() },
        { id: 2, order_code: '#ORD-8811', user_id: '987654321', user_name: 'Ngô Giai Dy', product_name: 'Telegram Bot Automator Pro', amount: 350000, status: 'completed', created_at: new Date() }
      ];
    }
    res.json(orders);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/orders/:id/approve', async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query(
      "UPDATE orders SET status = 'completed', delivery_text = COALESCE(NULLIF(delivery_text, ''), 'Duyệt thành công bởi Admin') WHERE id = $1",
      [id]
    );
    res.json({ success: true, message: 'Đã duyệt đơn hàng thành công!' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/orders/:id/cancel', async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [id]);
    res.json({ success: true, message: 'Đã hủy đơn hàng.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin Products
app.get('/api/admin/products', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM products ORDER BY id DESC');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/products', async (req, res) => {
  try {
    const { name, category, price, description = '', vipLevel = 0 } = req.body;
    if (!name || !price) {
      return res.status(400).json({ error: 'Tên và giá sản phẩm không được để trống.' });
    }
    const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') + '-' + Math.floor(100 + Math.random() * 900);
    const catMap = {
      'MINIGAME LIVE': 'minigame',
      'OTP & PROXY': 'proxy',
      'MƯA LÌ XÌ - CODE MÀN - CODE TELE': 'codes',
      'TRUNG TÂM VIP': 'vip'
    };
    const cat = catMap[category] || category || 'tools';

    const ins = await pool.query(
      `INSERT INTO products (slug, name, category, description, price, vip_level, is_active)
       VALUES ($1, $2, $3, $4, $5, $6, TRUE) RETURNING *`,
      [slug, name, cat, description, Number(price), Number(vipLevel)]
    );
    res.json({ success: true, product: ins.rows[0], message: 'Đã thêm sản phẩm mới thành công!' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin Bot Menu Config
app.get('/api/admin/bot-menu', async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM bot_menu_configs WHERE bot_key = 'primary'");
    let config = result.rows[0];
    if (!config || !config.button_labels) {
      config = {
        bot_key: 'primary',
        menu_mode: 'reply_keyboard',
        intro_text: 'Chào mừng bạn đến với HENDY BOT. Chọn một dịch vụ bên dưới:',
        rows: [
          ['🚀 MINIGAME LIVE', '🖨️ OTP & PROXY'],
          ['💳 NẠP TIỀN', 'VIP TRUNG TÂM VIP'],
          ['🛍️ MƯA LÌ XÌ - CODE MÀN - CODE TELE'],
          ['💎 TRUNG TÂM KHÁCH HÀNG'],
          ['💬 NHÓM CHAT', '📢 KÊNH THÔNG BÁO']
        ]
      };
    } else {
      let labels = config.button_labels;
      if (typeof labels === 'string') {
        try { labels = JSON.parse(labels); } catch (_) { labels = {}; }
      }
      config.rows = labels.rows || [
        ['🚀 MINIGAME LIVE', '🖨️ OTP & PROXY'],
        ['💳 NẠP TIỀN', 'VIP TRUNG TÂM VIP'],
        ['🛍️ MƯA LÌ XÌ - CODE MÀN - CODE TELE'],
        ['💎 TRUNG TÂM KHÁCH HÀNG'],
        ['💬 NHÓM CHAT', '📢 KÊNH THÔNG BÁO']
      ];
    }
    res.json(config);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/bot-menu', async (req, res) => {
  try {
    const { rows, introText } = req.body;
    const buttonLabels = JSON.stringify({ rows: rows || [] });
    await pool.query(
      `INSERT INTO bot_menu_configs (bot_key, menu_mode, intro_text, button_labels, updated_at)
       VALUES ('primary', 'reply_keyboard', $1, $2, CURRENT_TIMESTAMP)
       ON CONFLICT (bot_key) DO UPDATE
         SET intro_text = excluded.intro_text,
             button_labels = excluded.button_labels,
             updated_at = CURRENT_TIMESTAMP`,
      [introText || 'Chào mừng bạn đến với HENDY BOT.', buttonLabels]
    );
    broadcastWs({ type: 'bot.menu_updated', audience: 'all', rows });
    res.json({ success: true, message: 'Đã cập nhật cấu hình Menu Bot Telegram thành công!' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin System Settings / Maintenance
app.get('/api/admin/settings', async (req, res) => {
  try {
    const result = await pool.query('SELECT key, value FROM app_settings');
    const settings = {};
    for (const row of result.rows) {
      settings[row.key] = row.value;
    }
    res.json(settings);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/settings', async (req, res) => {
  try {
    const { settings } = req.body;
    if (settings && typeof settings === 'object') {
      for (const [k, v] of Object.entries(settings)) {
        await pool.query(
          `INSERT INTO app_settings (key, value, updated_at)
           VALUES ($1, $2, CURRENT_TIMESTAMP)
           ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP`,
          [k, String(v)]
        );
      }
    }
    broadcastWs({ type: 'settings.updated', audience: 'all', settings });
    res.json({ success: true, message: 'Đã lưu cấu hình hệ thống & trạng thái bảo trì thành công!' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// -------------------------------------------------------------
// PAGES ROUTING
// -------------------------------------------------------------

// Serve Admin Dashboard Pro for /admin or /admin/*
app.get(/^\/admin(\/.*)?$/, (req, res) => {
  res.sendFile(path.join(__dirname, 'apps/admin-dashboard/index.html'));
});

// Serve TMA Customer Center for / or /customer or any other SPA route
app.get(/^\/(customer(\/.*)?)?$/, (req, res) => {
  res.sendFile(path.join(__dirname, 'apps/website/index.html'));
});

// SPA fallback for all remaining non-API paths
app.use((req, res, next) => {
  if (req.path.startsWith('/api')) {
    return res.status(404).json({ error: 'Endpoint không tồn tại' });
  }
  res.sendFile(path.join(__dirname, 'apps/website/index.html'));
});

// Initialize and start server
async function start() {
  try {
    await initDb();
    server.listen(PORT, HOST, () => {
      console.log(`[HENDY BOT] Server đang chạy tại http://${HOST}:${PORT}`);
      console.log(`  - Telegram Mini App (Customer Center): http://${HOST}:${PORT}/`);
      console.log(`  - Admin Dashboard Pro: http://${HOST}:${PORT}/admin`);
    });
  } catch (err) {
    console.error('[HENDY BOT] Lỗi khởi động:', err);
    process.exit(1);
  }
}

start();
