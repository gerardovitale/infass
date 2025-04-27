import Link from 'next/link';

export default function NotFound() {
    return (
        <div className="flex items-center justify-center min-h-screen bg-gray-100">
            <div className="text-center p-8 bg-white shadow-lg rounded-lg">
                <h1 className="text-4xl font-bold text-red-600">
                    Product Not Found
                </h1>
                <p className="mt-4 text-lg text-gray-600">
                    Sorry, we couldn&apos;t find any product matching your
                    search.
                </p>
                <div className="mt-6">
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
