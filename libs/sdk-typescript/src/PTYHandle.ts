/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

import WebSocket from 'isomorphic-ws'
import { PTYResult, PTYControlMessage, PTYResizeMessage } from './types/PTY'
import { DaytonaError } from './errors/DaytonaError'

/**
 * PTY session handle for managing a single PTY session.
 *
 * Provides methods for sending input, resizing the terminal, and managing the WebSocket connection.
 */
export class PTYHandle {
  private _exitCode?: number
  private _error?: string
  private connected = false

  constructor(
    private readonly ws: WebSocket,
    private readonly handleKill: () => Promise<void>,
    private readonly onPty: (data: Uint8Array) => void | Promise<void>,
  ) {
    this.setupWebSocketHandlers()
  }

  /**
   * Exit code of the PTY process (if terminated)
   */
  get exitCode(): number | undefined {
    return this._exitCode
  }

  /**
   * Error message if the PTY failed
   */
  get error(): string | undefined {
    return this._error
  }

  /**
   * Check if connected to the PTY session
   */
  isConnected(): boolean {
    return this.connected && this.ws.readyState === WebSocket.OPEN
  }

  /**
   * Wait for the WebSocket connection to be established
   */
  async waitForConnection(): Promise<void> {
    if (this.isConnected()) {
      return
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('PTY connection timeout'))
      }, 10000) // 10 second timeout

      const checkConnection = () => {
        if (this.isConnected()) {
          clearTimeout(timeout)
          resolve()
        } else if (this.ws.readyState === WebSocket.CLOSED || this._error) {
          clearTimeout(timeout)
          reject(new Error(this._error || 'PTY connection failed'))
        } else {
          setTimeout(checkConnection, 100)
        }
      }

      checkConnection()
    })
  }

  /**
   * Send input data to the PTY
   */
  async sendInput(data: string | Uint8Array): Promise<void> {
    if (!this.isConnected()) {
      throw new Error('PTY is not connected')
    }

    try {
      if (typeof data === 'string') {
        this.ws.send(new TextEncoder().encode(data))
      } else {
        this.ws.send(data)
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      throw new Error(`Failed to send input to PTY: ${errorMessage}`)
    }
  }

  /**
   * Resize the PTY terminal
   */
  async resize(cols: number, rows: number): Promise<void> {
    if (!this.isConnected()) {
      throw new Error('PTY is not connected')
    }

    try {
      const resizeMessage: PTYResizeMessage = {
        type: 'resize',
        data: { cols, rows },
      }
      this.ws.send(JSON.stringify(resizeMessage))
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      throw new Error(`Failed to resize PTY: ${errorMessage}`)
    }
  }

  /**
   * Disconnect from the PTY session
   */
  async disconnect(): Promise<void> {
    if (this.ws) {
      try {
        this.ws.close()
      } catch {
        // Ignore close errors
      }
    }
  }

  /**
   * Wait for the PTY process to exit
   */
  async wait(): Promise<PTYResult> {
    return new Promise((resolve, reject) => {
      if (this._exitCode !== undefined) {
        resolve({
          exitCode: this._exitCode,
          error: this._error,
        })
        return
      }

      const checkExit = () => {
        if (this._exitCode !== undefined) {
          resolve({
            exitCode: this._exitCode,
            error: this._error,
          })
        } else if (this._error) {
          reject(new Error(this._error))
        } else {
          setTimeout(checkExit, 100)
        }
      }

      checkExit()
    })
  }

  async kill(): Promise<void> {
    return await this.handleKill()
  }

  private setupWebSocketHandlers(): void {
    // Set binary type for binary data handling
    if ('binaryType' in this.ws) {
      this.ws.binaryType = 'arraybuffer'
    }

    // Handle WebSocket open
    const handleOpen = async () => {
      this.connected = true
    }

    // Handle WebSocket messages
    const handleMessage = async (event: MessageEvent | any) => {
      try {
        const data = event && typeof event === 'object' && 'data' in event ? event.data : event

        if (typeof data === 'string') {
          // Handle JSON control messages
          try {
            const message: PTYControlMessage = JSON.parse(data)
            await this.handleControlMessage(message)
          } catch {
            // Not JSON, treat as regular text output
            if (this.onPty) {
              await this.onPty(new TextEncoder().encode(data))
            }
          }
        } else {
          // Handle binary data (terminal output)
          let bytes: Uint8Array

          if (data instanceof ArrayBuffer) {
            bytes = new Uint8Array(data)
          } else if (ArrayBuffer.isView(data)) {
            bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
          } else if (data instanceof Blob) {
            const buffer = await data.arrayBuffer()
            bytes = new Uint8Array(buffer)
          } else {
            throw new Error(`Unsupported message data type: ${Object.prototype.toString.call(data)}`)
          }

          if (this.onPty) {
            await this.onPty(bytes)
          }
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error)
        throw new Error(`Error handling PTY message: ${errorMessage}`)
      }
    }

    // Handle WebSocket errors
    const handleError = async (error: any) => {
      let errorMessage: string
      if (error instanceof Error) {
        errorMessage = error.message
      } else if (error && error instanceof Event) {
        errorMessage = 'WebSocket connection error'
      } else {
        errorMessage = String(error)
      }

      this._error = errorMessage
      this.connected = false
    }

    // Handle WebSocket close
    const handleClose = async () => {
      this.connected = false
    }

    // Attach event listeners based on WebSocket implementation
    if (this.ws.addEventListener) {
      // Browser WebSocket
      this.ws.addEventListener('open', handleOpen)
      this.ws.addEventListener('message', handleMessage)
      this.ws.addEventListener('error', handleError)
      this.ws.addEventListener('close', handleClose)
    } else if ('on' in this.ws && typeof this.ws.on === 'function') {
      // Node.js WebSocket
      this.ws.on('open', handleOpen)
      this.ws.on('message', handleMessage)
      this.ws.on('error', handleError)
      this.ws.on('close', handleClose)
    } else {
      throw new Error('Unsupported WebSocket implementation')
    }
  }

  private async handleControlMessage(message: PTYControlMessage): Promise<void> {
    switch (message.type) {
      case 'exit':
        this._exitCode = message.data?.code ?? 0
        this.connected = false
        // Don't call handleKill() here - the process exited naturally
        // handleKill() should only be used to manually terminate a running process
        break

      case 'error':
        this._error = message.data?.message ?? 'Unknown PTY error'
        this.connected = false
        throw new DaytonaError(this._error)

      default:
        // Unknown control message type, ignore
        break
    }
  }
}
