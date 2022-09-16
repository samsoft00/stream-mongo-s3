declare global {
    namespace NodeJS {
      interface ProcessEnv {
        DB_URL: string
        BUCKET: string
        MAX_POOL_SIZE: number
      }
    }
  }
  
  export {}