import { NextResponse } from 'next/server';
import dataStore from '../../../../../../lib/data-store.js';

export async function GET(request, { params }) {
  const { table } = params;
  const data = dataStore.getTableData(table);
  return NextResponse.json(data);
}
