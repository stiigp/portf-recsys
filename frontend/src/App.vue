<script setup>
import SearchBar from './components/SearchBar.vue'
</script>

<template>
  <div class="min-h-screen pt-10 px-4">
    
    <h1 class="text-3xl font-bold text-center mb-6">
      Movie Search
    </h1>

    <SearchBar />

  </div>
</template>

<style scoped>
.logo {
  height: 6em;
  padding: 1.5em;
  will-change: filter;
  transition: filter 300ms;
}
.logo:hover {
  filter: drop-shadow(0 0 2em #646cffaa);
}
.logo.vue:hover {
  filter: drop-shadow(0 0 2em #42b883aa);
}
</style>
