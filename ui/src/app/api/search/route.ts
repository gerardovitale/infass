import { getClient } from '@/auth/getClient';
import { logger } from '@/lib/logger';
import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const searchTerm = searchParams.get('search_term');
    const limit = searchParams.get('limit') || '20';
    const offset = searchParams.get('offset') || '0';

    if (!searchTerm) {
        return NextResponse.json({ error: 'search_term is required' }, { status: 400 });
    }

    try {
        const client = await getClient();
        const res = await client.fetch(
            `${process.env.API_BASE_URL}/products/search?search_term=${encodeURIComponent(searchTerm)}&limit=${limit}&offset=${offset}`
        );
        return NextResponse.json(res.data);
    } catch (error) {
        logger.error('Search API error', error instanceof Error ? error : undefined);
        return NextResponse.json({ error: 'Failed to fetch search results' }, { status: 502 });
    }
}
