import React from 'react';
import { SearchProduct } from './SearchProduct';
import Image from 'next/image';

export const Navbar = () => {
    return (
        <nav className="bg-white border-b border-gray-200 px-4 py-3 shadow-sm flex items-center justify-between">
            <div className="flex items-center">
                <Image
                    src="/images/vanta-logo.png" // Replace with your actual logo path
                    alt="Logo"
                    width={120} // Adjust width as needed
                    height={100} // Adjust height as needed
                />
            </div>
            <div className="flex-1 flex ml-4">
                <SearchProduct />
            </div>
        </nav>
    );
};
