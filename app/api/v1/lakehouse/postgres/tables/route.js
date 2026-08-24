import { NextResponse } from 'next/server';
import dataStore from '../../../../../../lib/data-store.js';

export async function GET() {
  const tables = dataStore.getWarehouseTables();
  return NextResponse.json(tables);
}
