'use client';

import { Product } from '@/types';
import { ProductImage } from '@/components/ProductImage/ProductImage';
import { useRouter } from 'next/navigation';
import { ProductDetailChart } from './ProductDetailChart';

type Props = {
    product: Product;
};
export const ProductDetail = ({ product }: Props) => {
    const router = useRouter();

    return (
        <div className="max-w-7xl mx-auto px-4 py-8">
            <button
                onClick={() => router.back()}
                className="mb-4 flex items-center text-gray-500 hover:text-gray-800 transition-colors"
                aria-label="Go back"
            >
                <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
                </svg>
            </button>
            <div className="grid grid-cols-1 md:grid-cols-12 gap-8">
                <div className="md:col-span-4 space-y-2">
                    <h1 className="text-2xl font-bold">{product.name}</h1>
                    <p className="text-gray-500">
                        {product.size} - {product.categories}
                    </p>
                    <p className="text-xl font-bold text-blue-600">
                        {product.current_price?.toFixed(2) ?? '-'}€
                    </p>
                    <ProductImage
                        src={product.image_url}
                        alt={product.name}
                        width={200}
                        height={200}
                    />
                </div>

                <div className="md:col-span-8 flex items-center justify-center">
                    {product.price_details?.length ? (
                        <ProductDetailChart data={product.price_details} />
                    ) : (
                        <div className="w-full h-64 bg-gray-100 rounded-xl flex items-center justify-center text-gray-400">
                            No data available
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};
