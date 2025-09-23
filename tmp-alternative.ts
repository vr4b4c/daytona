import { Daytona, Sandbox } from '@daytonaio/sdk'

async function ptyExec(sandbox: Sandbox) {
  const pty = await sandbox.process.createPTY({
    command: ['/bin/bash', '-l'],
    cols: 80,
    rows: 24,
  })
  console.log('PTY created', pty)

  let processExited = false

  const handler = await sandbox.process.connectPTY(pty, {
    onData: (data) => {
      // Proper way to decode UTF-8 bytes to text
      const text = new TextDecoder().decode(data)
      process.stdout.write(text) // Write directly to preserve terminal formatting
    },
    onConnect: () => {
      console.log('\n[DEBUG] PTY WebSocket connected')
    },
    onDisconnect: () => {
      console.log('\n[DEBUG] PTY WebSocket disconnected')
    },
    onExit: (exitCode, signal) => {
      console.log(`\n[DEBUG] PTY process exited with code ${exitCode}${signal ? ` (signal: ${signal})` : ''}`)
      processExited = true
    },
    onError: (error) => {
      console.error('\n[DEBUG] PTY error:', error)
    },
  })

  await handler.connect()

  await handler.sendInput('ls -la\n')
  // Small delay to let command execute
  await new Promise((resolve) => setTimeout(resolve, 1000))

  await handler.sendInput('echo "Hello PTY!"\n')
  await new Promise((resolve) => setTimeout(resolve, 500))

  await handler.resize(120, 30)
  await handler.sendInput('echo "Terminal resized"\n')
  await new Promise((resolve) => setTimeout(resolve, 500))

  await handler.sendInput('exit\n')

  // Wait for process to exit using our flag, with timeout
  console.log('\n[DEBUG] Waiting for process to exit...')
  const startTime = Date.now()
  const timeout = 5000 // 5 seconds

  while (!processExited && Date.now() - startTime < timeout) {
    await new Promise((resolve) => setTimeout(resolve, 100))
  }

  if (processExited) {
    console.log('[DEBUG] Process exited successfully')
  } else {
    console.log('[DEBUG] Timeout waiting for process exit, forcing disconnection')
  }

  console.log('[DEBUG] Disconnecting from PTY...')
  await handler.disconnect()
  console.log('[DEBUG] PTY disconnected successfully')
}

async function main() {
  const daytona = new Daytona()

  //  first, create a sandbox
  const sandbox = await daytona.create()

  try {
    await ptyExec(sandbox)
  } catch (error) {
    console.error('Error executing commands:', error)
  } finally {
    //  cleanup
    await daytona.delete(sandbox)
  }
}

main()
