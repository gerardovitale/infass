'use client';

import { useSearchParams } from 'next/navigation';

const SearchResult = () => {
    const searchParams = useSearchParams();
    const searchValue = searchParams.get('product') || '';

    return (
        <div className="flex flex-col items-center justify-center min-h-screen p-4">
            <h1 className="text-2xl font-bold text-gray-800">
                You searched: {searchValue}
            </h1>
        </div>
    );
};

export default SearchResult;
