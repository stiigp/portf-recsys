import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    host: '0.0.0.0', // Permite acesso fora do container
    port: 5173,      // Porta padrão do Vite
    watch: {
      usePolling: true // Força detecção de mudanças (crucial no Docker/WSL)
    }
  }
})
