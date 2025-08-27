import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
    /* config options here */
    images: {
        remotePatterns: [
            {
                protocol: 'https',
                hostname: 'prod-mercadona.imgix.net',
                pathname: '/images/***',
            },
        ],
    },
};

export default nextConfig;
