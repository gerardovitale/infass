// mocks/handlers.ts
import { http, HttpResponse } from 'msw';
import products from './responses/search.json';
export const handlers = [
    http.get(`${process.env.API_BASE_URL}/products/search`, ({ request }) => {
        const url = new URL(request.url);
        const limit = parseInt(url.searchParams.get('limit') || '20', 10);
        const offset = parseInt(url.searchParams.get('offset') || '0', 10);
        const sliced = products.results.slice(offset, offset + limit);
        return HttpResponse.json({
            ...products,
            results: sliced,
            total_results: products.results.length,
            limit,
            offset,
            has_more: offset + limit < products.results.length,
        });
    }),
    http.get(`${process.env.API_BASE_URL}/products/1`, () => {
        return HttpResponse.json(products.results[0]);
    }),
];
