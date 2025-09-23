/*
 * Copyright 2025 Daytona Platforms Inc.
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Sandbox } from '@daytonaio/api-client'
import {
  useReactTable,
  getCoreRowModel,
  getFacetedRowModel,
  getFacetedUniqueValues,
  getPaginationRowModel,
} from '@tanstack/react-table'
import { useMemo } from 'react'
import { FacetedFilterOption } from './types'
import { getColumns } from './columns'
import {
  SandboxFilters,
  SandboxSorting,
  convertApiSortingToTableSorting,
  convertApiFiltersToTableFilters,
  convertTableSortingToApiSorting,
  convertTableFiltersToApiFilters,
} from './types'

interface UseSandboxTableProps {
  data: Sandbox[]
  sandboxIsLoading: Record<string, boolean>
  writePermitted: boolean
  deletePermitted: boolean
  handleStart: (id: string) => void
  handleStop: (id: string) => void
  handleDelete: (id: string) => void
  handleArchive: (id: string) => void
  handleVnc: (id: string) => void
  getWebTerminalUrl: (id: string) => Promise<string | null>
  handleCreateSshAccess: (id: string) => void
  handleRevokeSshAccess: (id: string) => void
  pagination: {
    pageIndex: number
    pageSize: number
  }
  pageCount: number
  onPaginationChange: (pagination: { pageIndex: number; pageSize: number }) => void
  sorting: SandboxSorting
  onSortingChange: (sorting: SandboxSorting) => void
  filters: SandboxFilters
  onFiltersChange: (filters: SandboxFilters) => void
}

export function useSandboxTable({
  data,
  sandboxIsLoading,
  writePermitted,
  deletePermitted,
  handleStart,
  handleStop,
  handleDelete,
  handleArchive,
  handleVnc,
  getWebTerminalUrl,
  handleCreateSshAccess,
  handleRevokeSshAccess,
  pagination,
  pageCount,
  onPaginationChange,
  sorting,
  onSortingChange,
  filters,
  onFiltersChange,
}: UseSandboxTableProps) {
  // Convert API sorting and filters to table format for internal use
  const tableSorting = useMemo(() => convertApiSortingToTableSorting(sorting), [sorting])
  const tableFilters = useMemo(() => convertApiFiltersToTableFilters(filters), [filters])

  // TODO: empty at first, key value inputs added by user
  const labelOptions: FacetedFilterOption[] = useMemo(() => {
    const labels = new Set<string>()
    data.forEach((sandbox) => {
      Object.entries(sandbox.labels ?? {}).forEach(([key, value]) => {
        labels.add(`${key}: ${value}`)
      })
    })
    return Array.from(labels).map((label) => ({ label, value: label }))
  }, [data])

  // TODO: fetched from API
  const regionOptions: FacetedFilterOption[] = useMemo(() => {
    const regions = new Set<string>()
    data.forEach((sandbox) => {
      if (sandbox.target) {
        regions.add(sandbox.target)
      }
    })
    return Array.from(regions).map((region) => ({ label: region, value: region }))
  }, [data])

  const columns = useMemo(
    () =>
      getColumns({
        handleStart,
        handleStop,
        handleDelete,
        handleArchive,
        handleVnc,
        getWebTerminalUrl,
        sandboxIsLoading,
        writePermitted,
        deletePermitted,
        handleCreateSshAccess,
        handleRevokeSshAccess,
      }),
    [
      handleStart,
      handleStop,
      handleDelete,
      handleArchive,
      handleVnc,
      getWebTerminalUrl,
      sandboxIsLoading,
      writePermitted,
      deletePermitted,
      handleCreateSshAccess,
      handleRevokeSshAccess,
    ],
  )

  const table = useReactTable({
    data,
    columns,
    onColumnFiltersChange: (updater) => {
      const newTableFilters = typeof updater === 'function' ? updater(table.getState().columnFilters) : updater
      const newApiFilters = convertTableFiltersToApiFilters(newTableFilters)
      onFiltersChange(newApiFilters)
    },
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    onSortingChange: (updater) => {
      const newTableSorting = typeof updater === 'function' ? updater(table.getState().sorting) : updater
      const newApiSorting = convertTableSortingToApiSorting(newTableSorting)
      onSortingChange(newApiSorting)
    },
    getFacetedRowModel: getFacetedRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(),
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
    pageCount: pageCount,
    onPaginationChange: (updater) => {
      const newPagination = typeof updater === 'function' ? updater(table.getState().pagination) : updater
      onPaginationChange(newPagination)
    },
    state: {
      sorting: tableSorting,
      columnFilters: tableFilters,
      pagination: {
        pageIndex: pagination.pageIndex,
        pageSize: pagination.pageSize,
      },
    },
    defaultColumn: {
      size: 100,
    },
    enableRowSelection: deletePermitted,
    getRowId: (row) => row.id,
  })

  return {
    table,
    labelOptions,
    regionOptions,
  }
}
