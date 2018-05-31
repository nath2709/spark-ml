package sparkEx.ml

import java.net.URL;
import java.io.InputStreamReader;
import com.rometools.rome.io.SyndFeedInput
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.HttpGet
import com.rometools.rome.io.XmlReader
import com.rometools.rome.feed.synd.SyndEntry
import java.util.function.Consumer
import java.io.BufferedWriter
import java.io.FileWriter

object Newsfeed {

  def main(args: Array[String]): Unit = {

    val urlmap = List("sports" -> "https://timesofindia.indiatimes.com/rssfeeds/4719161.cms",
      "education" -> "https://timesofindia.indiatimes.com/rssfeeds/913168846.cms",
      "business" -> "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
      "tech" -> "https://timesofindia.indiatimes.com/rssfeeds/5880659.cms",
      "health" -> "https://timesofindia.indiatimes.com/rssfeeds/3908999.cms")

    val html = """<a.+\/a>""".r
    val client = HttpClients.createMinimal()
    var request: HttpGet = null

    val writer = new BufferedWriter(new FileWriter("data.csv"))
    urlmap.foreach(f => {

      println(f._1 + " " + f._2)
      request = new HttpGet(f._2)
      val response = client.execute(request)
      val stream = response.getEntity().getContent()
      val input = new SyndFeedInput();
      val feed = input.build(new XmlReader(stream));
      val entries = feed.getEntries
      println(entries.size())
      val entriesItr = entries.iterator()

      while (entriesItr.hasNext()) {
        val entry = entriesItr.next()
        val description = entry.getDescription.getValue
        println(f._1 + " => " + description)
        val temp = html.replaceAllIn(description, "")

        if (!temp.isEmpty()) {
          println(f._1 + " => " + temp)
          writer.write(f._1 + " = " + temp)
          writer.write("\n")
        }

      }

    })
    writer.flush()
    writer.close()

  }
}