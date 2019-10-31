package ru.nsu.fit.search

import kotlinx.coroutines.*
import java.io.File
import java.io.FileInputStream
import java.io.RandomAccessFile
import java.lang.Long.max
import java.lang.Long.min
import java.lang.RuntimeException
import java.nio.file.Files
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.streams.asStream

private const val COROUTINE_SCOPE_NAME = "patternSearchScope"
private const val BYTE_VALUES = 256
private const val PRIME_NUM = 101

private const val BATCH_SIZE = 32 * 1024 * 1024

private val processors = Runtime.getRuntime().availableProcessors()
@ObsoleteCoroutinesApi
val scope = CoroutineScope(newFixedThreadPoolContext(processors, COROUTINE_SCOPE_NAME))

enum class SearchBy {
    NAME,
    CONTENT
}

@ObsoleteCoroutinesApi
suspend fun search(root: File, pattern: String, searchBy: SearchBy) {
    when(searchBy) {
        SearchBy.NAME -> searchByName(root, pattern)
        SearchBy.CONTENT -> searchByContent(root, pattern)
    }
}


private fun searchByName(root: File, pattern: String) {
    root.walkTopDown().asStream().parallel()
            .filter { it.isValid() && pattern in it.name }
            .forEach {
                println(it.absolutePath)
            }
}

@ObsoleteCoroutinesApi
private suspend fun searchByContent(root: File, pattern: String) {
    val binaryPattern = pattern.toByteArray()
    val batch = mutableListOf<File>()
    var len = 0L
    root.walkBottomUp().forEach { file ->
        if (len < BATCH_SIZE) {
            if (!file.isValid() || binaryPattern.size > file.length()) {
                return@forEach
            }
            len += file.length()
            batch.add(file)
        } else {
            searchInBatch(batch, binaryPattern)
            batch.clear()
            len = 0L
        }
    }
    if (len > 0L) {
        searchInBatch(batch, binaryPattern)
    }
}

@ObsoleteCoroutinesApi
private suspend fun searchInBatch(batch: List<File>, pattern: ByteArray) {
    val threads = min(processors.toLong(), max(1L, batch.sumByLong { it.length() } / (pattern.size))).toInt()
    val threadFiles = distributeFiles(threads, pattern.size, batch)
    val res = searchParallel(scope, threadFiles, pattern)
    printResults(res)
}

private fun printResults(found: ConcurrentMap<File, Long>) {
    found.forEach { (file, pos) ->
        println("${file.absolutePath}:$pos")
    }
}

private suspend fun searchParallel(scope: CoroutineScope,
                                   threadFiles: List<List<FilePart>>,
                                   pattern: ByteArray): ConcurrentMap<File, Long> {
    val found = ConcurrentHashMap<File, Long>()
    threadFiles.map { files ->
        scope.async {
            files.forEach { part->
                search(part, pattern, found)
            }
        }
    }.awaitAll()
    return found
}

private fun distributeFiles(threads: Int, patternLen: Int, files: List<File>): List<List<FilePart>> {
    val dataSize = files.sumByLong { it.length() }
    val partition = distribute(threads, dataSize)
    val rest = files.map { it.part() }.toCollection(Stack())
    return partition.map { partSize ->
        var size = 0L
        val res = mutableListOf<FilePart>()
        while (size < partSize) {
            val candidate = rest.pop()
            val free = partSize - size
            if (free >= candidate.len) {
                res.addPart(candidate, patternLen)
                size += candidate.len
                continue
            }
            val part = candidate.file.part(candidate.pos, free)
            val restPart = candidate.file.part(candidate.pos + free, candidate.len - free)
            res.addPart(part, patternLen)
            size += part.len
            rest.add(restPart)
        }
        res
    }
}

private fun MutableList<FilePart>.addPart(part: FilePart, patternLen: Int) {
    if (part.pos != 0L) {
        val overhead = patternLen - 1
        val start = part.pos - overhead
        add(part.file.part(max(0, start), part.len + if (start >= 0) overhead.toLong() else overhead + start))
        return
    }
    add(part)
}

private fun distribute(threads: Int, dataSize: Long): LongArray {
    val partition = LongArray(threads)
    val part = dataSize / threads
    var rest = dataSize % threads
    for (i in 0 until threads) {
        partition[i] = if (rest > 0) {
            --rest
            part + 1
        } else {
            part
        }
    }
    return partition
}

private fun search(filePart: FilePart, pattern: ByteArray, found: ConcurrentHashMap<File, Long>) {
    if (found.contains(filePart.file)) {
        return
    }
    val randAccessFile = RandomAccessFile(filePart.file, "r")
    randAccessFile.seek(filePart.pos)
    FileInputStream(randAccessFile.fd).buffered().use { input ->
        var readCount = 0L
        fun read(): Byte {
            val res = input.read()
            if (res < 0) {
                throw RuntimeException("End of file reached $filePart")
            }
            ++readCount
            return res.toByte()
        }
        val candidate = ByteArray(pattern.size)
        var patternHash = 0
        var textHash = 0
        var h = 1
        repeat(pattern.size - 1) {
            h = (h * BYTE_VALUES) % PRIME_NUM
        }
        for (i in pattern.indices) {
            candidate[i] = read()
            patternHash = (BYTE_VALUES * patternHash + pattern[i]) % PRIME_NUM
            textHash = (BYTE_VALUES * textHash + candidate[i]) % PRIME_NUM
        }
        var start = 0
        for (i in 0..(filePart.len - pattern.size )) {
            if (found.contains(filePart.file)) {
                return
            }
            if (patternHash == textHash) {
                var j = 0
                while(j < pattern.size) {
                    if (candidate[(start + j) % pattern.size] != pattern[j]) {
                        break
                    }
                    ++j
                }
                if (j == pattern.size) {
                    found[filePart.file] = filePart.pos + i
                    return
                }
            }
            if (i < filePart.len - pattern.size) {
                val first = candidate[start]
                val pos = (start + pattern.size) % pattern.size
                candidate[pos] = read()
                textHash = (BYTE_VALUES * (textHash - first * h) + candidate[pos]) % PRIME_NUM
                if (textHash < 0) {
                    textHash += PRIME_NUM
                }
                start = (start + 1) % pattern.size
            }
        }
    }
}

private fun File.isValid() = this.isFile && !Files.isSymbolicLink(this.toPath())

private fun File.part(pos: Long = 0L, len: Long = this.length()) = FilePart(this, pos, len)

private data class FilePart(val file: File, val pos: Long, val len: Long)
