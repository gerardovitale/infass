'use client';

import { Product } from '@/types';
import Image from 'next/image';
import { ProductDetailChart } from './ProductDetailChart';

type Props = {
    product: Product;
};
export const ProductDetail = ({ product }: Props) => {
    const fallbackImage = '/images/default-image.png';

    return (
        <div className="max-w-7xl mx-auto px-4 py-8">
            <div className="grid grid-cols-1 md:grid-cols-12 gap-8">
                <div className="md:col-span-4 space-y-2">
                    <h1 className="text-2xl font-bold">{product.name}</h1>
                    <p className="text-gray-500">
                        {product.size} - {product.category}
                    </p>
                    <p className="text-xl font-bold text-blue-600">
                        {product.price.toFixed(2)}€
                    </p>
                    <Image
                        src={fallbackImage}
                        alt={product.name}
                        width={200}
                        height={200}
                    />
                </div>

                <div className="md:col-span-8 flex items-center justify-center">
                    {product.priceDetails.length > 0 && (
                        <ProductDetailChart data={product.priceDetails} />
                    )}
                    {product.priceDetails.length == 0 && (
                        <div className="w-full h-64 bg-gray-100 rounded-xl flex items-center justify-center text-gray-400">
                            No data available
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};
