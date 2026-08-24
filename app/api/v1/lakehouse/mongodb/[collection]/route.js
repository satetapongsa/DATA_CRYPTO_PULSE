import { NextResponse } from 'next/server';
import dataStore from '../../../../../../lib/data-store.js';

export async function GET(request, { params }) {
  const { collection } = params;
  const data = dataStore.getMongoDocs(collection);
  return NextResponse.json(data);
}
