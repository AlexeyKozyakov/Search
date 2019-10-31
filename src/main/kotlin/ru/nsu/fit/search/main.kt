package ru.nsu.fit.search

import kotlinx.coroutines.ObsoleteCoroutinesApi
import java.io.File
import kotlin.time.ExperimentalTime

@ExperimentalTime
@ObsoleteCoroutinesApi
suspend fun main(args: Array<String>) {
    if (args.size < 2) {
        printUsage()
        return
    }
    args[0].takeIf { it == "--data" || it == "--name" }?.let {
        val searchBy = if (it == "--data") SearchBy.CONTENT else SearchBy.NAME
        val pattern = args[1]
        val file = if (args.size > 2) File(args[2]) else File("./")
        if (!file.exists()) {
            println("File doesn't exist")
            return
        }
        search(file, pattern, searchBy)
    } ?: printUsage()
}

private fun printUsage() {
    println("Usage:\njava <jvmArgs> -jar ./search.jar --data|--name <pattern> <folder|file>")
}
