'use client';

import { useState } from 'react';
import Image from 'next/image';

const FALLBACK_IMAGE = '/images/default-image.png';
const ALLOWED_HOSTNAMES = ['prod-mercadona.imgix.net'];

function isAllowedSrc(src?: string): boolean {
    if (!src) return false;
    try {
        const { hostname } = new URL(src);
        return ALLOWED_HOSTNAMES.includes(hostname);
    } catch {
        return false;
    }
}

type Props = {
    src?: string;
    alt: string;
    width: number;
    height: number;
};

export const ProductImage = ({ src, alt, width, height }: Props) => {
    const initialSrc = isAllowedSrc(src) ? src! : FALLBACK_IMAGE;
    const [imgSrc, setImgSrc] = useState(initialSrc);

    return (
        <Image
            src={imgSrc}
            alt={alt}
            width={width}
            height={height}
            onError={() => setImgSrc(FALLBACK_IMAGE)}
        />
    );
};
