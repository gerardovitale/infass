import React from 'react';
import { SearchProduct } from './SearchProduct';

export const Navbar = () => {
    return (
        <nav className="bg-white border-b border-gray-200 px-4 py-3 shadow-sm">
            <SearchProduct />
        </nav>
    );
};
