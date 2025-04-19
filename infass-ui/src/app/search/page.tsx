'use client';

import SearchResult from '@/components/Search/SearchResult';
import { Suspense } from 'react';

const SearchPage = () => {
    return (
        <Suspense>
            <SearchResult />
        </Suspense>
    );
};

export default SearchPage;
