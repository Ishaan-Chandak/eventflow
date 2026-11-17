import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/actuator': 'http://localhost:8080',
      '/demo': { target: 'http://localhost:8082', rewrite: path => path.replace(/^\/demo/, '') },
    },
  },
})
