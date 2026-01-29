<template>
  <div v-if="recommendations.length > 0" class="mt-8">
    <h2 class="text-2xl font-bold mb-4 text-gray-200">Recommended based on this</h2>
    <div class="relative group">
      <button 
        @click="scrollLeft"
        class="absolute left-0 top-1/2 -translate-y-1/2 z-10 bg-white/80 p-2 rounded-full shadow-lg opacity-0 group-hover:opacity-100 transition-opacity hidden md:block hover:bg-white"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6 text-gray-800" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
        </svg>
      </button>


      <div 
        ref="scrollContainer"
        class="flex gap-4 overflow-x-auto pb-4 scrollbar-hide scroll-smooth"
      >
         <div 
          v-for="movie in recommendations" 
          :key="movie.movie_id" 
          @click="goToMovie(movie.movie_id)"
          class="flex-none w-32 md:w-40 cursor-pointer transition-transform hover:scale-105"
        >
          <img 
            :src="posterUrl(movie.poster_path)" 
            :alt="movie.title"
            class="w-full h-48 md:h-60 object-cover rounded-lg shadow-md bg-gray-200"
            loading="lazy"
          />
          
          <p class="mt-2 text-sm font-medium text-gray-500 truncate text-center">
            {{ movie.title }}
          </p>
        </div>
      </div>

      <button 
        @click="scrollRight"
        class="absolute right-0 top-1/2 -translate-y-1/2 z-10 bg-white/80 p-2 rounded-full shadow-lg opacity-0 group-hover:opacity-100 transition-opacity hidden md:block hover:bg-white"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6 text-gray-800" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
        </svg>
      </button>
      
    </div>
  </div>
</template>


<script setup>
import { ref, watch, onMounted } from 'vue'
import axios from 'axios'
import { useRouter } from 'vue-router'

const props = defineProps({
  movieId: {
    type: [String, Number],
    required: true
  }
})

const recommendations = ref([])
const scrollContainer = ref(null)
const router = useRouter()

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const fetchRecommendations = async (id) => {
  try {
    const res = await axios.get(`${API_BASE_URL}/hb/${id}/10`)

    recommendations.value = res.data.recommendations
  } catch (error) {
    console.error('Erro ao buscar recomendações:', error)
  }
}

const posterUrl = (path) => path ? `https://image.tmdb.org/t/p/w342${path}` : 'https://via.placeholder.com/342x513?text=Sem+Imagem'

const goToMovie = (id) => {  
  router.push(`/movie/${id}`)
  window.scrollTo({ top: 0, behavior: 'smooth' })
}

const scrollRight = () => {
  if (scrollContainer.value) {
    scrollContainer.value.scrollBy({ left: 300, behavior: 'smooth' })
  }
}

const scrollLeft = () => {
  if (scrollContainer.value) {
    scrollContainer.value.scrollBy({ left: -300, behavior: 'smooth' })
  }
}

onMounted(() => {
  if (props.movieId) fetchRecommendations(props.movieId)
})

watch(() => props.movieId, (newId) => {
  if (newId) fetchRecommendations(newId)
})
</script>

<style scoped>
.scrollbar-hide::-webkit-scrollbar {
    display: none;
}
.scrollbar-hide {
    -ms-overflow-style: none;
    scrollbar-width: none;
}
</style>
