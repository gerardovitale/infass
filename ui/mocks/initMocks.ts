// lib/initMocks.ts
export async function initMocks() {
  console.log('Initializing API mocks:', process.env.USE_API_MOCKS);
    if (
      process.env.NODE_ENV === 'development' &&
      process.env.USE_API_MOCKS === 'true'
    ) {
      console.log('Using API mocks in development mode');
      const { server } = await import('./node');
      server.listen({
        onUnhandledRequest: 'warn',
      });
    }
  }
