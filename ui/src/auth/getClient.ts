import { GoogleAuth } from 'google-auth-library';

let cachedClient;

export const getClient = async () => {
    if (cachedClient) {
        return cachedClient;
    }
    const auth = new GoogleAuth();
    return await auth.getIdTokenClient(process.env.API_BASE_URL || '');
};
