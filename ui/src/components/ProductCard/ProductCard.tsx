'use client';

import { Product } from '@/types';
import { ProductImage } from '@/components/ProductImage/ProductImage';
import Link from 'next/link';

type Props = {
    product: Product;
};
export const ProductCard = ({ product }: Props) => {
    return (
        <Link href={`/product/${product.id}`}>
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <ProductImage
                        src={product.image_url}
                        alt="Product Image"
                        width={100}
                        height={100}
                    />
                    <div>
                        <h3 className="text-lg font-semibold text-gray-900">
                            {product.name}
                        </h3>
                        <p className="text-sm text-gray-500">{product.size}</p>
                    </div>
                </div>
                <span className="text-blue-600 font-bold text-lg">
                    €{product.current_price?.toFixed(2) ?? '-'}
                </span>
            </div>
        </Link>
    );
};
