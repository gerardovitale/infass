'use client';

import Link from 'next/link';
import { useState } from 'react';

const handleEnter = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
        e.preventDefault();
        const searchButton = document.getElementById('search-button');
        searchButton?.click();
    }
};

export const SearchProduct = () => {
    const [searchValue, setSearchValue] = useState<string>('');

    return (
        <div className="flex items-center justify-center w-full max-w-md mx-auto">
            <input
                id="search-input"
                type="text"
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
                placeholder="Search products..."
                className="w-full max-w-md px-4 py-2 border rounded-xl focus:outline-none focus:ring focus:ring-blue-300"
                onKeyDown={(e) => handleEnter(e)}
            />
            <Link
                id="search-button"
                href={{
                    pathname: '/search',
                    query: { product: encodeURIComponent(searchValue.trim()) },
                }}
                aria-label="Search"
                className="ml-2 px-4 py-2 text-white bg-blue-600 rounded-xl hover:bg-blue-700 focus:outline-none focus:ring focus:ring-blue-300"
            >
                <svg
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    strokeWidth={1.5}
                    stroke="currentColor"
                    className="size-6"
                >
                    <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z"
                    />
                </svg>
            </Link>
        </div>
    );
};
