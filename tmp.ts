import { Daytona, Sandbox } from '@daytonaio/sdk'

async function ptyExec(sandbox: Sandbox) {
  const handler = await sandbox.process.createPTY({
    id: 'test-pty',
    command: ['/bin/bash', '-l'],
    cols: 80,
    rows: 24,
    onData: (data) => {
      // Proper way to decode UTF-8 bytes to text
      const text = new TextDecoder().decode(data)
      process.stdout.write(text) // Write directly to preserve terminal formatting
    },
  })

  await handler.sendInput('ls -la\n')
  await handler.sendInput('echo "Hello PTY!"\n')

  await handler.resize(120, 30)

  await handler.sendInput('exit\n')
  await handler.wait()
  await handler.disconnect()

  console.log('PTY exited with code:', handler.exitCode)
  console.log('PTY error:', handler.error)
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
