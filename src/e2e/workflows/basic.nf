
workflow{
    Channel.fromPath('oci:///nomad-storage/*')
        | filter { it.name.endsWith('.csv') }
        | view
}