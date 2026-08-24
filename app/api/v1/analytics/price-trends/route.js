import { NextResponse } from 'next/server';
import dataStore from '../../../../../lib/data-store.js';

export async function GET(request) {
  const { searchParams } = new URL(request.url);
  const symbol = searchParams.get('symbol') || 'BTC';
  const days = parseInt(searchParams.get('days') || '30', 10);
  const data = dataStore.getPriceTrends(symbol, days);
  return NextResponse.json(data);
}
