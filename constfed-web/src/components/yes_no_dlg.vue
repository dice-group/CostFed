<template>
    <div class="yndlg">
        <p>Are you sure?</p>
        <input type="submit" @click="on_yes" value="Yes"/>
        <input type="submit" @click="on_no" value="No"/>
    </div>
</template>

<script>
export default {
    name: 'vue-yes-no-dlg',
    props: {action: undefined},
    data () {
        return {}
    },
    mounted() {

    },
    methods: {
        on_yes() {
            this.$props.action();
            this.$emit('vuedals:close');
        },
        on_no() {
            this.$emit('vuedals:close');
        }
    }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.yndlg {
    border-radius: 5px;
    background-color: #f2f2f2;
    padding: 20px;
}

input[type=submit] {
    width: 70px;
    background-color: #4CAF50;
    color: white;
    padding: 14px 20px;
    margin: 8px 0;
    border: none;
    border-radius: 4px;
    cursor: pointer;
}
</style>

<style>
.vuedal header {
    border-bottom: 0px;
}
</style>
