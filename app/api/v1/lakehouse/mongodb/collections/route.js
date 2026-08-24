import { NextResponse } from 'next/server';
import dataStore from '../../../../../../lib/data-store.js';

export async function GET() {
  const collections = dataStore.getMongoCollections();
  return NextResponse.json(collections);
}
