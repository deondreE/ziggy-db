import * as net from 'net';

const SERVER_IP = '127.0.0.1';
const SERVER_PORT = 8080;
const BUFFER_SIZE = 4096;
const TIMEOUT_SECS = 3;

function sendAndReceive(socket: net.Socket, command: string): Promise<void> {
  return new Promise((resolve, reject) => {
    let commandWithNewline = command.endsWith('\n') ? command : command + '\n';

    console.log(`\n>>> ${command.trim()}`);
    socket.write(commandWithNewline);

    const handleData = (data: Buffer) => {
      const response = data.toString('utf8').trim();
      if (response.length > 0) {
        console.log(`<<< ${response}`);
      } else {
        console.log('(no response)');
      }
      cleanup();
    };

    const handleTimeout = () => {
      console.log('timeout waiting for response');
      cleanup();
    };

    const handleError = (err: Error) => {
      cleanup();
      reject(err);
    };

    const cleanup = () => {
      socket.removeListener('data', handleData);
      socket.removeListener('timeout', handleTimeout);
      socket.removeListener('error', handleError);
      resolve();
    };

    socket.once('data', handleData);
    socket.once('timeout', handleTimeout);
    socket.once('error', handleError);

    socket.setTimeout(TIMEOUT_SECS * 1000);
  });
}

async function connectToZiggy(ip: string, port: number): Promise<void> {
  console.log(`Connecting to ${ip}:${port}...`);

  const socket = new net.Socket();

  return new Promise((resolve, reject) => {
    socket.connect(port, ip, async () => {
      try {
        await sendAndReceive(socket, 'SET foo bar');
        await sendAndReceive(socket, 'GET foo');
        await sendAndReceive(socket, 'SET x 42');
        await sendAndReceive(socket, 'EXIT');

        console.log('Testing done.\n');
        socket.end();
        resolve();
      } catch (err) {
        socket.destroy();
        reject(err);
      }
    });

    socket.on('error', (err) => {
      if ((err as NodeJS.ErrnoException).code === 'ECONNREFUSED') {
        console.error(`Connection refused. Is it running at ${ip}:${port}?`);
      } else {
        console.error(`Client error: ${err.message}`);
      }
      socket.destroy();
      reject(err);
    });
  });
}

(async () => {
  try {
    await connectToZiggy(SERVER_IP, SERVER_PORT);
  } catch (err) {
    console.error('Client error:', err);
    process.exit(1);
  }
})();
