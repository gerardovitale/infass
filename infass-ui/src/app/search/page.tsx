import SearchResult from '@/components/Search/SearchResult';
import React from 'react';
import { Suspense } from 'react';
import { PageProps } from '../../../.next/types/app/layout';

export default async function SearchPage({ searchParams }: PageProps) {
    const { product } = await searchParams;

    const searchValue = product?.trim() ?? '';
    return (
        <Suspense>
            <SearchResult productSearched={searchValue} />
        </Suspense>
    );
}
