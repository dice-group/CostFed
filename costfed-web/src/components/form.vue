<template>
    <div class="epform">
        <label>Address</label>
        <input v-model="formdata.address" placeholder="sparql endpoint address">
        
        <label>Description</label>
        <textarea v-model="formdata.description" placeholder="description"></textarea>
        
        <input type="submit" @click="save" v-bind:disabled="!formdata.address" v-bind:value="formdata.mode == 'add' ? 'Add' : 'Save'"/>
    </div>
</template>

<script>
export default {
    name: 'vue-form',
    props: {controller: undefined, selection: undefined, mode: undefined},
    data () {
        return {
            formdata: {address: undefined, description: '', mode: undefined}
        }
    },
    mounted() {
        if (this.$props.selection) {
            this.formdata.address = this.$props.selection.address;
            this.formdata.description = this.$props.selection.description;
        }
        this.formdata.mode = this.$props.mode;
    },
    methods: {
        save() {
            this.$props.controller.proc_endpoint(this.formdata);
            this.$emit('vuedals:close');
        }
    }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.epform {
    border-radius: 5px;
    background-color: #f2f2f2;
    padding: 20px;
}

.epform input, textarea {
    width: 100%;
    resize: none;
    padding: 12px 20px;
    margin: 8px 0;
    display: inline-block;
    border: 1px solid #ccc;
    border-radius: 4px;
    box-sizing: border-box;
    -moz-box-sizing: border-box;
    -webkit-box-sizing: border-box;
    
}

input[type=submit] {
    width: 100%;
    background-color: #4CAF50;
    color: white;
    padding: 14px 20px;
    margin: 8px 0;
    border: none;
    border-radius: 4px;
    cursor: pointer;
}

input[disabled] {
    background-color: lightgray;
}
</style>

<style>
.vuedal header {
    border-bottom: 0px;
}
</style>
