import { Daytona, Sandbox, PTYHandle } from '@daytonaio/sdk'

async function ptyExec(sandbox: Sandbox) {
  const ptySessionId = 'test-pty'
  const handler = await sandbox.process.createPTY({
    id: ptySessionId,
    cols: 80,
    rows: 24,
    onData: (data) => {
      // Proper way to decode UTF-8 bytes to text
      const text = new TextDecoder().decode(data)
      process.stdout.write(`[handler1-start] ${text} [handler1-end]\n`) // Write directly to preserve terminal formatting
    },
  })
  await handler.sendInput('exit\n')
  await new Promise((resolve) => setTimeout(resolve, 2000))
  await handler.disconnect()

  let handler3: PTYHandle
  try {
    handler3 = await sandbox.process.connectPTY(ptySessionId, {
      onData: (data) => {
        const text = new TextDecoder().decode(data)
        process.stdout.write(`[handler3-start] ${text} [handler3-end]\n`)
      },
    })
    await handler3.sendInput('echo "Hello PTY 3!"\n')
    await handler3.wait()
  } catch (error) {
    console.error('Error executing commands:', error)
  } finally {
    console.log(handler3!.error)
  }
}

async function main() {
  const daytona = new Daytona()

  const sandboxes = await daytona.list()
  for (const sandbox of sandboxes) {
    await daytona.delete(sandbox)
  }

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
