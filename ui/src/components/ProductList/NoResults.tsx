'use client';

const NoResults = () => {
    return (
        <div className="text-center py-12">
            <h2 className="text-2xl font-semibold text-gray-800 mb-4">
                No products found
            </h2>
            <p className="text-gray-600 mb-6">
                We couldn&apos;t find any products matching your search. Please
                try a different search term or check back later.
            </p>
        </div>
    );
};

export default NoResults;
