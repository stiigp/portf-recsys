<template>
  <div class="max-w-4xl mx-auto p-6">
    <div v-if="loading" class="text-center py-10">Loading movie...</div>
    <div v-else-if="error" class="text-red-500 text-center">{{ error }}</div>
    <div v-else-if="movie" class="flex flex-col md:flex-row gap-8">
      <img 
        :src="posterUrl(movie.poster_path)" 
        alt="Poster" 
        class="w-64 rounded-lg shadow-lg self-center md:self-start"
      />

      <div class="flex-1">
        <h1 class="text-4xl font-bold mb-2">{{ movie.title }}</h1>
        <p class="text-gray-500 text-lg mb-4">
          {{ movie.release_date?.split('-')[0] }} • {{ movie.runtime }} min
        </p>

        <p class="text-gray-100 leading-relaxed mb-6">
          {{ movie.overview }}
        </p>
        
        <div class="flex gap-2 mb-6">
          <span v-for="genre in movie.genres" :key="genre.id" 
                class="bg-blue-100 text-blue-800 px-3 py-1 rounded-full text-sm">
            {{ genre.name }}
          </span>
        </div>
      </div>
    </div>

    <RecommendationsView :movieId="route.params.movieId"/>

  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import RecommendationsView from '../components/RecommendationsCarousel.vue';
import axios from 'axios';

const route = useRoute()
const movie = ref(null)
const loading = ref(true)
const error = ref(null)

const TMDB_API_KEY = import.meta.env.VITE_TMDB_API_KEY
const TMDB_BASE_URL = 'https://api.themoviedb.org/3'
const API_BASE_URL = import.meta.env.VITE_API_URL

const posterUrl = (path) => path ? `https://image.tmdb.org/t/p/w500${path}` : 'https://via.placeholder.com/300x450'

const fetchMovie = async (id) => {
  loading.value = true
  error.value = null

  try {
    const res_internal_api = await axios.get(`${API_BASE_URL}/movie/${id}`)
    
    const tmdbId = res_internal_api.data.tmdbId

    const header = {
      accept: "application/json",
      Authorization: `Bearer ${TMDB_API_KEY}`
    }

    const res = await axios.get(`${TMDB_BASE_URL}/movie/${tmdbId}`, { headers: header })
    
    movie.value = res.data
  } catch (err) {
    if (err.response) {
      error.value = `Error ${err.response.status}: ${err.response.data.status_message || err.message}`
    } else if (err.request) {
      error.value = "Connection error: server did not answer"
    } else {
      error.value = err.message
    }
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchMovie(route.params.movieId)
})

watch(() => route.params.movieId, (newId) => {
  if (newId) fetchMovie(newId)
})
</script>
