<template>
    <div id="real-app">
    <h2>AKSW/CostFed</h2>
        <vue-tabs active-tab-color="#9b59b6" active-text-color="white">
            <v-tab title="Endpoints">
                <vue-costfed-endpoints>
                </vue-costfed-endpoints>
            </v-tab>
<!--
            <v-tab title="SPARQL">
                <vuetable ref="vuetable"
                    api-url="https://vuetable.ratiw.net/api/users"
                    :fields="fields"
                    pagination-path=""
                    @vuetable:pagination-data="onPaginationData"
                ></vuetable>
                <vuetable-pagination ref="pagination" @vuetable-pagination:change-page="onChangePage"></vuetable-pagination>
                
            </v-tab>
            -->
        </vue-tabs>
  </div>
</template>

<script>
import Vue from 'vue'
import VueTabs from 'vue-nav-tabs'
import Endpoints from './components/endpoints.vue'
//import {Vuetable, VuetablePagination} from 'vuetable-2'

import 'vue-nav-tabs/dist/vue-tabs.min.css'

Vue.use(VueTabs);
//Vue.use(Vuetable);

Vue.component('vue-costfed-endpoints', Endpoints);
//Vue.component('vuetable', Vuetable);
//Vue.component('vuetable-pagination', VuetablePagination);

export default {
    name: 'app',
    data() {
        return { fields: ['name', 'email','birthdate','nickname','gender','__slot:actions'] }
  },
  //components:{
  // 'vuetable-pagination': Vuetable.VuetablePagination
  //},
  methods: {
    //onPaginationData (paginationData) {
    //  this.$refs.pagination.setPaginationData(paginationData)
    //},
    //onChangePage (page) {
   //   this.$refs.vuetable.changePage(page)
    //},
    //editRow(rowData){
    //  alert("You clicked edit on"+ JSON.stringify(rowData))
    //},
    //deleteRow(rowData){
    //  alert("You clicked delete on"+ JSON.stringify(rowData))
    //}
  }
}

</script>

<style>
#real-app {
  //font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  //text-align: center;
  //color: #2c3e50;
} 

</style>
