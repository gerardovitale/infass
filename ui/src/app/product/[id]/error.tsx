'use client';

import Link from 'next/link';

export default function Error({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
    return (
        <div className="flex items-center justify-center min-h-screen bg-gray-100">
            <div className="text-center p-8 bg-white shadow-lg rounded-lg">
                <h1 className="text-4xl font-bold text-red-600">Something went wrong</h1>
                <p className="mt-4 text-lg text-gray-600">
                    Sorry, we couldn&apos;t load the product details. Please try again later.
                </p>
                <div className="mt-6 flex gap-4 justify-center">
                    <button
                        onClick={reset}
                        className="px-6 py-2 bg-gray-600 text-white font-semibold rounded-lg shadow-md hover:bg-gray-700"
                    >
                        Try again
                    </button>
                    <Link
                        href={'/'}
                        className="px-6 py-2 bg-blue-600 text-white font-semibold rounded-lg shadow-md hover:bg-blue-700"
                    >
                        Go Back to Home
                    </Link>
                </div>
            </div>
        </div>
    );
}
