import SearchResult from '@/components/Search/SearchResult';
import { Suspense } from 'react';
import { PageProps } from '../../../.next/types/app/layout';

export default async function SearchPage({ searchParams }: PageProps) {
    const { search_term } = await searchParams;

    const searchValue = search_term?.trim() ?? '';
    return (
        <Suspense>
            <SearchResult productSearched={searchValue} />
        </Suspense>
    );
}
