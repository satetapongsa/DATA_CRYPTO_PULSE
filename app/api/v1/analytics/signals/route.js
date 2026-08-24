import { NextResponse } from 'next/server';
import dataStore from '../../../../../lib/data-store.js';

export async function GET() {
  const data = dataStore.getSignals();
  return NextResponse.json(data);
}
