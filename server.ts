import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface SignalingPayload {
  type: string;
  id?: string;
  senderId?: string;
  targetId?: string;
  target?: string;
  instanceId?: string;
  offerId?: string;
  data?: unknown;
  sdp?: unknown;
  candidate?: unknown;
  role?: string;
  peerId?: string;
  nodeId?: string;
  [key: string]: unknown;
}

function normalizeNodeId(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return '';
  if (trimmed.toUpperCase() === 'MASTER') return 'MASTER';
  return trimmed.replace(/^sigma-node-/i, '').trim();
}

async function startServer() {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  const server = http.createServer(app);
  const PORT = Number(process.env.PORT) || 3000;

  // ============================================================================
  // 1. MULTI-CLIENT SWITCHBOARD REGISTRY (`clients = new Map<string, WebSocket>()`)
  // ============================================================================
  const clients = new Map<string, WebSocket>();
  const clientInstances = new Map<string, string>(); // nodeId -> unique tab/device instanceId
  const masterSockets = new Set<WebSocket>();
  const socketToId = new Map<WebSocket, string>();
  const httpMailboxes = new Map<string, SignalingPayload[]>();

  const allocateUnique4DigitId = (): string => {
    for (let attempt = 0; attempt < 200; attempt++) {
      const candidate = String(Math.floor(1000 + Math.random() * 9000));
      const existingWs = clients.get(candidate);
      if (!existingWs || existingWs.readyState !== WebSocket.OPEN) {
        return candidate;
      }
    }
    return String(Math.floor(1000 + Math.random() * 9000));
  };

  const broadcastToAllMasters = (payload: Record<string, unknown>) => {
    const serialized = JSON.stringify(payload);
    let sent = false;
    masterSockets.forEach((mWs) => {
      if (mWs.readyState === WebSocket.OPEN) {
        try {
          mWs.send(serialized);
          sent = true;
        } catch {}
      }
    });
    if (!sent) {
      const primaryMaster = clients.get('MASTER');
      if (primaryMaster && primaryMaster.readyState === WebSocket.OPEN) {
        try {
          primaryMaster.send(serialized);
          sent = true;
        } catch {}
      }
    }
    return sent;
  };

  const isAnyMasterOnline = (): boolean => {
    for (const mWs of masterSockets) {
      if (mWs.readyState === WebSocket.OPEN) return true;
    }
    const primary = clients.get('MASTER');
    return Boolean(primary && primary.readyState === WebSocket.OPEN);
  };

  const enqueueMailbox = (targetId: string, packet: SignalingPayload) => {
    const queue = httpMailboxes.get(targetId) || [];
    queue.push(packet);
    if (queue.length > 50) queue.shift();
    httpMailboxes.set(targetId, queue);
  };

  const wss = new WebSocketServer({ noServer: true });
  const hmrWss = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) => {
      if (protocols.has('vite-hmr')) return 'vite-hmr';
      if (protocols.has('vite-ping')) return 'vite-ping';
      const first = protocols.values().next().value;
      return first || false;
    },
  });

  server.on('upgrade', (request, socket, head) => {
    socket.on('error', () => {});
    const protocolHeader = String(
      request.headers['sec-websocket-protocol'] || ''
    );
    const reqUrl = request.url || '/';

    if (
      protocolHeader.includes('vite-hmr') ||
      protocolHeader.includes('vite-ping') ||
      reqUrl.includes('token=') ||
      reqUrl.includes('__vite')
    ) {
      hmrWss.handleUpgrade(request, socket, head, (ws) => {
        ws.on('error', () => {});
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'connected' }));
        }
        ws.on('message', (msg) => {
          try {
            const parsed = JSON.parse(msg.toString());
            if (parsed?.type === 'ping' && ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ type: 'pong' }));
            }
          } catch {}
        });
      });
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  });

  wss.on('connection', (ws: WebSocket) => {
    console.log('[SERVER-WS] Koneksi WebSocket baru masuk.');

    ws.on('message', (rawMessage) => {
      let data: SignalingPayload | null = null;
      try {
        data = JSON.parse(rawMessage.toString()) as SignalingPayload;
      } catch (err) {
        console.error('[SERVER-WS] Gagal parse JSON:', err);
        return;
      }

      if (!data || typeof data !== 'object' || typeof data.type !== 'string') {
        return;
      }

      // ======================================================================
      // ATURAN 1: REGISTRASI MULTI-KLIEN TANPA SALING TIMPA ID
      // ======================================================================
      if (
        data.type === 'register' ||
        data.type === 'REGISTER' ||
        data.type === 'register_master' ||
        data.type === 'register_client'
      ) {
        const rawId = String(
          data.id ||
            data.senderId ||
            (data.type === 'register_master' ? 'MASTER' : '')
        );
        if (!rawId) {
          console.warn('[SERVER-WS] Pesan register ditolak karena id kosong!');
          return;
        }

        let normalizedId = normalizeNodeId(rawId);
        const incomingInstanceId = String(data.instanceId || '');

        // Jika klien baru mencoba mendaftar dengan ID yang SUDAH dipakai oleh klien lain yang masih aktif
        // (misal karena scan QR code yang sama atau duplikasi tab), otomatis berikan ID 4-digit baru yang unik!
        if (normalizedId !== 'MASTER') {
          const existingWs = clients.get(normalizedId);
          const existingInstanceId = clientInstances.get(normalizedId);
          const isOccupiedByAnotherClient =
            existingWs &&
            existingWs !== ws &&
            existingWs.readyState === WebSocket.OPEN &&
            (!incomingInstanceId ||
              !existingInstanceId ||
              incomingInstanceId !== existingInstanceId);

          if (isOccupiedByAnotherClient) {
            const oldId = normalizedId;
            normalizedId = allocateUnique4DigitId();
            console.log(
              `[SERVER-WS] COLLISION PREVENTED: ID "${oldId}" sedang dipakai klien aktif lain -> Klien baru dialihkan ke ID unik "${normalizedId}"`
            );
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(
                JSON.stringify({
                  type: 'id_reassigned',
                  oldId,
                  newId: normalizedId,
                  senderId: 'MASTER',
                  targetId: normalizedId,
                  masterOnline: isAnyMasterOnline(),
                })
              );
            }
          }
        }

        // Jika socket ini sebelumnya terdaftar dengan ID berbeda, bersihkan ID lama
        const previousIdForSocket = socketToId.get(ws);
        if (
          previousIdForSocket &&
          previousIdForSocket !== normalizedId &&
          previousIdForSocket !== 'MASTER' &&
          clients.get(previousIdForSocket) === ws
        ) {
          clients.delete(previousIdForSocket);
          clientInstances.delete(previousIdForSocket);
        }

        clients.set(normalizedId, ws);
        socketToId.set(ws, normalizedId);
        if (incomingInstanceId && normalizedId !== 'MASTER') {
          clientInstances.set(normalizedId, incomingInstanceId);
        }
        if (normalizedId === 'MASTER') {
          masterSockets.add(ws);
        }

        const activeClientIds = Array.from(clients.keys()).filter(
          (k) => k !== 'MASTER' && clients.get(k)?.readyState === WebSocket.OPEN
        );

        console.log(
          `[SERVER-WS] REGISTERED: id="${normalizedId}" | Master Online: ${isAnyMasterOnline()} | Active Clients (${
            activeClientIds.length
          }): [${activeClientIds.join(', ')}]`
        );

        if (normalizedId === 'MASTER') {
          // Beri tahu semua Client yang sedang aktif bahwa MASTER sudah online
          clients.forEach((clientWs, clientId) => {
            if (
              clientId !== 'MASTER' &&
              clientWs.readyState === WebSocket.OPEN
            ) {
              clientWs.send(
                JSON.stringify({
                  type: 'master_online',
                  senderId: 'MASTER',
                  targetId: clientId,
                })
              );
            }
          });
        } else {
          // Beri tahu Client yang baru register status MASTER
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(
              JSON.stringify({
                type: 'registered_ack',
                id: normalizedId,
                masterOnline: isAnyMasterOnline(),
                senderId: 'MASTER',
                targetId: normalizedId,
              })
            );
            if (isAnyMasterOnline()) {
              ws.send(
                JSON.stringify({
                  type: 'master_online',
                  senderId: 'MASTER',
                  targetId: normalizedId,
                })
              );
            }
          }

          // Beri tahu MASTER bahwa ada Klien aktif yang siap di-handshake
          broadcastToAllMasters({
            type: 'NEW_CLIENT_JOINED',
            clientId: normalizedId,
            senderId: normalizedId,
            targetId: 'MASTER',
            timestamp: Date.now(),
          });
        }
        return;
      }

      // Dukungan Dynamic Role Assignment (`REQUEST_ROLE`) dengan alokasi ID 4-digit unik
      if (data.type === 'REQUEST_ROLE') {
        const masterOnline = isAnyMasterOnline();
        if (!masterOnline) {
          ws.send(
            JSON.stringify({
              type: 'ROLE_ASSIGNED',
              role: 'MASTER',
              peerId: 'MASTER',
              masterId: 'MASTER',
            })
          );
        } else {
          const uniqueClientId = allocateUnique4DigitId();
          ws.send(
            JSON.stringify({
              type: 'ROLE_ASSIGNED',
              role: 'CLIENT',
              peerId: uniqueClientId,
              masterId: 'MASTER',
            })
          );
        }
        return;
      }

      // Pastikan pengirim otomatis terdaftar di Map jika belum sempat mengirim `register`
      const rawSenderId = String(
        data.senderId || data.id || data.nodeId || socketToId.get(ws) || ''
      );
      const senderKey = normalizeNodeId(rawSenderId);

      if (senderKey) {
        if (!clients.has(senderKey) || clients.get(senderKey)?.readyState !== WebSocket.OPEN) {
          clients.set(senderKey, ws);
          socketToId.set(ws, senderKey);
          if (senderKey === 'MASTER') {
            masterSockets.add(ws);
          }
        }
      }

      // ======================================================================
      // ATURAN 2: ROUTING SINKRON SPESIFIK PER KLIEN (`targetId`)
      // ======================================================================
      const rawTargetId = String(data.targetId || data.target || '');
      if (!rawTargetId) {
        return;
      }

      const targetId = normalizeNodeId(rawTargetId);

      const normalizedPacket: SignalingPayload = {
        ...data,
        senderId: senderKey || data.senderId,
        targetId,
        data:
          data.data !== undefined
            ? data.data
            : data.sdp !== undefined
            ? data.sdp
            : data.candidate !== undefined
            ? data.candidate
            : undefined,
      };

      if (targetId === 'MASTER') {
        const delivered = broadcastToAllMasters(normalizedPacket);
        if (!delivered) {
          enqueueMailbox('MASTER', normalizedPacket);
        }
      } else {
        const targetWs = clients.get(targetId);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify(normalizedPacket));
        } else {
          enqueueMailbox(targetId, normalizedPacket);
        }
      }
    });

    ws.on('close', () => {
      masterSockets.delete(ws);
      const closedId = socketToId.get(ws);
      socketToId.delete(ws);

      if (closedId && clients.get(closedId) === ws) {
        if (closedId === 'MASTER' && masterSockets.size > 0) {
          const nextMasterWs = Array.from(masterSockets).find(
            (s) => s.readyState === WebSocket.OPEN
          );
          if (nextMasterWs) {
            clients.set('MASTER', nextMasterWs);
            return;
          }
        }

        clients.delete(closedId);
        clientInstances.delete(closedId);
        console.log(
          `[SERVER-WS] DISCONNECTED: id="${closedId}" dihapus dari Map. Sisa peer: [${Array.from(
            clients.keys()
          ).join(', ')}]`
        );

        if (closedId !== 'MASTER') {
          broadcastToAllMasters({
            type: 'client_disconnected',
            senderId: closedId,
            targetId: 'MASTER',
          });
        }
      }
    });

    ws.on('error', (err) => {
      console.error('[SERVER-WS] Socket error:', err);
    });
  });

  // Jalur HTTP Signaling Fallback (/api/signal)
  app.post('/api/signal', (req, res) => {
    const data = req.body as SignalingPayload;
    if (!data || typeof data !== 'object') {
      res.status(400).json({ ok: false });
      return;
    }
    const rawTarget = String(data.targetId || data.target || '');
    const targetId = normalizeNodeId(rawTarget);

    if (targetId === 'MASTER') {
      const delivered = broadcastToAllMasters(data);
      if (!delivered) {
        enqueueMailbox('MASTER', data);
      }
    } else if (targetId) {
      const targetWs = clients.get(targetId);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(JSON.stringify(data));
      } else {
        enqueueMailbox(targetId, data);
      }
    }
    res.json({ ok: true, masterOnline: isAnyMasterOnline() });
  });

  app.get('/api/signal/:targetId', (req, res) => {
    const rawTarget = req.params.targetId;
    const targetId = normalizeNodeId(rawTarget);
    const messages = httpMailboxes.get(targetId) || [];
    httpMailboxes.set(targetId, []);
    res.json({
      ok: true,
      messages,
      masterOnline: isAnyMasterOnline(),
    });
  });

  app.get('/api/health', (_req, res) => {
    const activeIds = Array.from(clients.keys());
    res.json({
      status: 'ok',
      masterOnline: isAnyMasterOnline(),
      connectedIds: activeIds,
      clientsCount: activeIds.filter((id) => id !== 'MASTER').length,
    });
  });

  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: false,
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(
      `[SERVER] S.I.G.M.A. Multi-Peer Signaling Server listening on http://0.0.0.0:${PORT}`
    );
  });
}

startServer();
