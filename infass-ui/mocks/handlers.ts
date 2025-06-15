// mocks/handlers.ts
import { http, HttpResponse } from 'msw';
import * as products from "./responses/search.json";
export const handlers = [
    http.get(`${process.env.API_BASE_URL}/products/search`, () => {
     return  HttpResponse.json(products);
  }),
    http.get(`${process.env.API_BASE_URL}/products/1`, () => {
        return HttpResponse.json(products.results[0]);
    }),
];
