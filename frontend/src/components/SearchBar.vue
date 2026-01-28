<template>
  <div class="relative w-full max-w-3xl mx-auto">
    <!-- Barra de Pesquisa -->
    <div class="relative">
      <input
        type="text"
        v-model="query"
        @input="handleInput"
        placeholder="Search movies..."
        class="w-full px-4 py-3 text-gray-700 bg-gray-50 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:bg-white transition-colors duration-200"
      />
      
      <!-- Ícone de Loading (opcional) -->
      <div v-if="loading" class="absolute right-3 top-3">
        <svg class="animate-spin h-5 w-5 text-gray-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
      </div>
    </div>

    <!-- Dropdown de Autocomplete -->
    <ul
      v-if="suggestions.length > 0 && showDropdown"
      class="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-60 overflow-y-auto"
    >
      <li
        v-for="movie in suggestions"
        :key="movie.tmdbId || movie.id"
        @click="selectMovie(movie)"
        class="px-4 py-2 hover:bg-blue-50 cursor-pointer flex justify-between items-center transition-colors duration-150"
      >
        <img
          :src="posterUrl(movie)"
          :alt="`Poster de ${movie.title}`"
          class="w-14 h-20 object-cover rounded bg-gray-200 flex-shrink-0"
          loading="lazy"
        />
        <span class="text-sm font-medium text-gray-800">{{ movie.title }}</span>
        <span v-if="movie.year" class="text-xs text-gray-500">({{ movie.year }})</span>
      </li>
    </ul>

    <!-- Mensagem de "Nenhum resultado" (opcional) -->
    <div 
      v-if="query.length >= 3 && suggestions.length === 0 && !loading && hasSearched"
      class="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-center text-gray-500 text-sm"
    >
      No movies found.
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { useRouter } from 'vue-router'

const query = ref('')
const suggestions = ref([])
const loading = ref(false)
const showDropdown = ref(false)
const hasSearched = ref(false)
const router = useRouter()
let debounceTimeout = null

const API_URL = import.meta.env.API_URL || 'http://localhost:8000'

const posterUrl = (movie) =>
  movie.poster_path ? `https://image.tmdb.org/t/p/w92${movie.poster_path}` : null

const fetchSuggestions = async (searchTerm) => {
  if (searchTerm.length < 3) {
    suggestions.value = []
    showDropdown.value = false
    return
  }

  loading.value = true
  hasSearched.value = true

  try {
    // encodeURIComponent é importante para tratar espaços e caracteres especiais
    const response = await fetch(`${API_URL}/autocomplete/${encodeURIComponent(searchTerm)}`)
    
    if (!response.ok) throw new Error('Erro na API')
    
    const data = await response.json()

    suggestions.value = data
    showDropdown.value = true
  } catch (error) {
    console.error('Erro ao buscar filmes:', error)
    suggestions.value = []
  } finally {
    loading.value = false
  }
}

const handleInput = () => {
  // Limpa timeout anterior se o usuário continuar digitando
  if (debounceTimeout) clearTimeout(debounceTimeout)
  
  // Se limpar o input, reseta tudo
  if (query.value.length < 3) {
    suggestions.value = []
    showDropdown.value = false
    loading.value = false
    hasSearched.value = false
    return
  }

  // Agenda nova busca para 200ms
  debounceTimeout = setTimeout(() => {
    fetchSuggestions(query.value)
  }, 200)
}

const selectMovie = (movie) => {
  query.value = movie.title
  showDropdown.value = false
  
  router.push({ 
    name: 'movie-detail', 
    params: { tmdbId: movie.tmdbId }
  })

  console.log('Filme selecionado:', movie)
}

</script>
