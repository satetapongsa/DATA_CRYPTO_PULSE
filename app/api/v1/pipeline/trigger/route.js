import { NextResponse } from 'next/server';
import dataStore from '../../../../../lib/data-store.js';

export async function POST() {
  const result = dataStore.triggerDag();
  return NextResponse.json(result);
}
