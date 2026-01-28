import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '../views/HomeView.vue'
import MovieDetailView from '../views/MovieDetailView.vue'

const routes = [
  { path: '/', component: HomeView },
    
  { path: '/movie/:tmdbId', name: 'movie-detail', component: MovieDetailView }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
