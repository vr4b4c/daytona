/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

import { PTYCreateRequest, PTYCreateResponse, PTYSessionInfo, PTYListResponse } from '@daytonaio/api-client'

// Branded types for type safety
declare const __brand: unique symbol
type Brand<B> = { [__brand]: B }
export type Branded<T, B> = T & Brand<B>

export type Stdout = Branded<string, 'stdout'>
export type Stderr = Branded<string, 'stderr'>
export type PTYOutput = Branded<Uint8Array, 'pty'>

/**
 * PTY control message types for WebSocket communication
 */
export interface PTYControlMessage {
  type: 'resize' | 'ping' | 'pong' | 'exit' | 'error'
  data?: any
}

/**
 * PTY resize message
 */
export interface PTYResizeMessage extends PTYControlMessage {
  type: 'resize'
  data: {
    cols: number
    rows: number
  }
}

/**
 * PTY exit message
 */
export interface PTYExitMessage extends PTYControlMessage {
  type: 'exit'
  data: {
    code: number
    signal?: string
  }
}

/**
 * PTY error message
 */
export interface PTYErrorMessage extends PTYControlMessage {
  type: 'error'
  data: {
    message: string
  }
}

/**
 * Options for creating a PTY session
 */
export interface PTYCreateOptions {
  /**
   * The unique identifier for the PTY session
   */
  id: string

  /**
   * Command to run in the PTY session
   */
  command?: string[]

  /**
   * Working directory for the PTY session
   */
  workDir?: string

  /**
   * Environment variables for the PTY session
   */
  env?: Record<string, string>

  /**
   * Number of terminal columns
   */
  cols?: number

  /**
   * Number of terminal rows
   */
  rows?: number
}

/**
 * Options for connecting to a PTY session
 */
export interface PTYConnectOptions {
  /**
   * Callback to handle PTY output data
   */
  onData?: (data: Uint8Array) => void | Promise<void>

  /**
   * Request timeout in seconds
   */
  timeoutSec?: number
}

/**
 * PTY session result
 */
export interface PTYResult {
  /**
   * Exit code when the PTY process ends
   */
  exitCode?: number

  /**
   * Error message if the PTY failed
   */
  error?: string
}

/**
 * Re-export API client types for convenience
 */
export type { PTYCreateRequest, PTYCreateResponse, PTYSessionInfo, PTYListResponse }
