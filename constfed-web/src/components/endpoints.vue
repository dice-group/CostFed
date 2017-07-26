<template>
  <div id="endpoints">
    <table class="epoints" cellspacing=0 cellpadding=0 width="100%">
        <thead>
            <tr>
                <th>Enable</th>
                <th>Address</th>
                <th>Description</th>
                <th>Summary</th>
            </tr>
        </thead>
        
        <tr v-for="(value, key, index) in endpoints"
            v-bind:class="[key == row_select ? 'row_select' : '', key == row_over ? 'row_over' : '', (index % 2 == 1) ? 'row_odd' : '']"
            @click="row_click(key)" @mouseenter="row_mouseenter(key)" @mouseleave="row_mouseleave(key)">
                <td class="cbstl"><input type="checkbox" :checked="value.enable == 'true'" @click="on_toggle(key)"/></td>
                <td width="0">{{value.address}}</td>
                <td width="100%">{{value.description}}</td>
                <td class="sum">
                        <div v-if="value.summary=='no'">
                            <img src="../assets/minus.png"/>
                            <button @click="on_build(key)">Build</button>
                        </div>
                        <div v-if="value.summary=='error'">
                            <img width="24" src="../assets/error.png"/>
                            <a target="_blank" :href="host + 'endpoints?action=get-error&id=' + key" ><img src="../assets/edwnl.png"/></a>
                            <button @click="on_build(key)">Build</button>
                        </div>
                        <div v-if="value.summary=='ready'">
                            <img src="../assets/ok.png"/>
                            <a target="_blank" :href="host + 'endpoints?action=get-summary&id=' + key"><img src="../assets/dwnl.png"/></a>
                            <button @click="on_build(key)">Rebuild</button>
                        </div>
                        <div v-if="value.summary=='building'">
                            <img width="24" src="../assets/loading.gif"/>
                            <span>{{value["summary-progress"]}}%</span>
                            <button @click="on_build_stop(key)">Stop</button>
                        </div>
                </td>
        </tr>
    </table>
    <br/>
    <div class="buttons">
        <button @click="on_add_new">Add New</button>
        <button @click="on_edit" v-bind:disabled="!row_select">Edit</button>
        <button @click="on_remove" v-bind:disabled="!row_select">Remove</button>
    </div>
    <vuedal></vuedal>
  </div>
</template>

<script>
import Vue from 'vue'
import axios from 'axios'
import {default as Vuedals, Component as Vuedal, Bus as VuedalsBus} from 'vuedals';
import EPForm from './form.vue'
import YNDlg from './yes_no_dlg.vue'

Vue.use(Vuedals);

export default {
    name: 'vue-costfed-endpoints',
    data () {
        return { host: 'http://localhost:8080/costfed-web/', endpoints: {}, row_over: '', row_select: undefined, selection: undefined }}
    ,
    created() {
        this.loadEndpoints(this.host + 'endpoints');
        setInterval(function () {
          this.loadEndpoints(this.host + 'endpoints');
        }.bind(this), 1000); 
    },
    components: {Vuedal, EPForm, YNDlg},
    methods: {
        row_click(k) {
            if (this.row_select == k) {
                this.row_select = undefined;
                this.selection = undefined;
            } else {
                this.row_select = k;
                this.selection = this.endpoints[k];
            }
        },
        row_mouseenter(k) {
            this.row_over = k;
        },
        row_mouseleave(k) {
            if (this.row_over == k) {
                this.row_over = '';
            };
        },
        proc_endpoint(formdata) {
            if (formdata.mode == 'add') {
                this.row_select = undefined;
                this.selection = undefined;
            } else if (formdata.mode == 'edit') {
                this.selection.address = formdata.address;
                this.selection.description = formdata.description;
            }
            this.loadEndpoints(this.host + 'endpoints', {
                id: this.row_select,
                action: formdata.mode,
                address: formdata.address,
                description: formdata.description,
            });
            console.log(formdata.mode + ' endpoint: ' + formdata.address);
        },
        remove_endpoint() {
            console.log('removing endpoint: ' + this.row_select);
            this.loadEndpoints(this.host + 'endpoints', {
                action: 'remove',
                id: this.row_select
            });
            this.row_select = undefined;
            this.selection = undefined;
        },
        on_toggle(k) {
            console.log('on_toggle endpoint: ' + k);
            this.loadEndpoints(this.host + 'endpoints', {
                action: 'toggle',
                id: k
            });
        },
        on_build(k) {
            console.log('on_build endpoint: ' + k);
            this.loadEndpoints(this.host + 'endpoints', {
                action: 'build-summary',
                id: k
            });
        },
        on_build_stop(k) {
            console.log('on_build endpoint: ' + k);
            this.loadEndpoints(this.host + 'endpoints', {
                action: 'stop-build-summary',
                id: k
            });
        },
        on_add_new() {
            VuedalsBus.$emit('new', {
                name: 'showing-form',
                props: {controller: this, selection: this.selection, mode: 'add'},
                title: 'Add SPARQL Endpoint',
                component: EPForm
            });
        },
        on_edit() {
            VuedalsBus.$emit('new', {
                name: 'showing-form',
                props: {controller: this, selection: this.selection, mode: 'edit'},
                title: 'Edit SPARQL Endpoint',
                component: EPForm
            });
        },
        on_remove() {
            VuedalsBus.$emit('new', {
                name: 'showing-form',
                props: {action: this.remove_endpoint},
                title: 'You are about to remove \'' + this.selection.address + '\' endpoint.',
                component: YNDlg
            });
        },
        loadEndpoints(url, params) {
            //console.log('loading!!!');
            var vm = this;
            axios.get(url, {params: params})
                .then(function (response) {
                    vm.endpoints = response.data;
                })
                .catch(function (error) {
                    console.log(error);
                });
        },
        click() {
            this.loadEndpoints(this.host + 'endpoints');
            console.log('click')
        }
   },
}


</script>

<style scoped>

#endpoints {
    margin: 10px 0px 10px 0px;
}

#endpoints table {
    border-top: 1px solid gray;
    border-left: 1px solid gray;
    
  //font-family: 'Avenir', Helvetica, Arial, sans-serif;
  //-webkit-font-smoothing: antialiased;
  //-moz-osx-font-smoothing: grayscale;
  //text-align: center;
  //color: #2c3e50;
} 

#endpoints th {
padding: 0px 5px 0px 5px;
    border-right: 1px solid gray;
    border-bottom: 1px solid gray;
}

.row_odd {
    background-color: #f3f3f3;
}

.row_over {
    background-color: #e3e3e3;
}

.row_select {
    background-color: #d3d3ff;
}

#endpoints td {
    border-right: 1px solid gray;
    border-bottom: 1px solid gray;
    padding: 5px 5px 5px 5px;
}

#endpoints h2 {
        text-align: left;
        margin: 0px 0px 10px 0px;
        //border: 1px black thin;
}

#endpoints thead {
    background: linear-gradient(#fff,#d3d3d3);
}

#endpoints .buttons {
    float:right;
    background: linear-gradient(#fff,#d3d3d3);
}

.cbstl {
    text-align: center;
}

.sum {
    text-align: center;
}

.sum div {
    width: 120px;
}

.sum div img {
    vertical-align: middle;
}

</style>
