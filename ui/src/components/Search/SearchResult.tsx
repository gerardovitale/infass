import { getClient } from '@/auth/getClient';
import { logger } from '@/lib/logger';
import { ProductResponse } from '@/types';
import { unstable_cache } from 'next/cache';
import NoResults from '../ProductList/NoResults';
import InfiniteProductList from '../ProductList/InfiniteProductList';

const SEARCH_LIMIT = 20;
const CACHE_REVALIDATE_SECONDS = 300;

type Props = {
    productSearched: string;
};

const fetchSearchResults = unstable_cache(
    async (searchTerm: string) => {
        const client = await getClient();
        const res = await client.fetch(
            `${process.env.API_BASE_URL}/products/search?search_term=${searchTerm}&limit=${SEARCH_LIMIT}&offset=0`
        );
        return res.data as ProductResponse;
    },
    ['product-search'],
    { revalidate: CACHE_REVALIDATE_SECONDS }
);

export default async function SearchResult(props: Props) {
    try {
        const response = await fetchSearchResults(props.productSearched);
        if (!response || response.total_results === 0) {
            return <NoResults />;
        }

        return (
            <InfiniteProductList
                initialProducts={response.results}
                initialHasMore={response.has_more}
                searchTerm={props.productSearched}
                limit={SEARCH_LIMIT}
            />
        );
    } catch (error) {
        logger.error('Error fetching search results', error instanceof Error ? error : undefined);
        return <NoResults />;
    }
}
