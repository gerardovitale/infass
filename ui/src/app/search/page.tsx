import SearchResult from '@/components/Search/SearchResult';
import { Spinner } from '@/components/Spinner/Spinner';
import { Suspense } from 'react';
import { PageProps } from '../../../.next/types/app/layout';

export default async function SearchPage({ searchParams }: PageProps) {
    const { search_term } = await searchParams;

    const searchValue = search_term?.trim() ?? '';
    return (
        <Suspense fallback={<Spinner size="lg" />}>
            <SearchResult productSearched={searchValue} />
        </Suspense>
    );
}
