import com.github.javafaker.Faker
import org.apache.commons.cli.*
import org.postgresql.Driver
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import kotlin.system.exitProcess
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.random.Random.Default.nextBoolean

val defaultSizes = mapOf(
    "country" to 7,
    "team" to 50,
    "team_title" to 100,
    "player_title" to 100,
    "competition" to 15
)
val YEAR_OF_FOUNDATION_RANGE = 1900..2019
val TITLES_PER_PLAYER = 0..5
val TITLES_PER_TEAM = 0..5
const val BIRTHDAY_FROM = "1980-01-01"
const val BIRTHDAY_TO = "2002-01-01"
const val START_YEAR = 2018
const val PLAYERS_NUMBER = 21
const val TEAMS_PER_COMPETITION = 30
const val SEASONS_COUNT = 4
val POINTS_RANGE = 0..100
val GOALS_SCORED_MATCH_RANGE = 0..4
val COMPETITION_NAME_LENGTH_RANGE = 6..15
val TEAM_NAME_LENGTH_RANGE = 5..8
val TEAM_TITLE_LENGTH_RANGE = 5..8
val PLAYER_TITLE_LENGTH_RANGE = 5..8
val GOALS_PER_PLAYER_RANGE = 0..2
val YELLOW_CARDS_PER_PLAYER_RANGE = 0..2
val DASHES_PER_PLAYER_RANGE = 0..5
val SAVES_PER_PLAYER_RANGE = 0..5
val TACKLES_PER_PLAYER_RANGE = 0..5

val faker = Faker()
val connection = connect()

val tablesMap = mutableMapOf<String, Int?>()
val tablesList = listOf("competition", "country", "match", "match_statistics", "player", "player_title",
        "player_titles", "season", "standings", "team", "team_title", "team_titles")
var truncate = false
val parser = SimpleDateFormat("yyyy-MM-dd")

fun main(args: Array<String>) {
    parseArgs(args)

    if (truncate) {
        connection.createStatement().executeUpdate("truncate ${tablesList.joinToString()} cascade")
    }

    for (i in 1..tablesMap["country"]!!) {
        connection.createStatement().executeUpdate(
                "insert into country values (default, $$${faker.address().country()}$$)"
        )
    }

    for (i in 1..tablesMap["team_title"]!!) {
        connection.createStatement().executeUpdate(
                "insert into team_title values (default, $$${randomString(TEAM_TITLE_LENGTH_RANGE.random())}$$)"
        )
    }

    for (i in 1..tablesMap["player_title"]!!) {
        connection.createStatement().executeUpdate(
                "insert into player_title values (default, $$${randomString(PLAYER_TITLE_LENGTH_RANGE.random())}$$)"
        )
    }

    for (i in 1..tablesMap["team"]!!) {
        val teamId = executeSQL("select nextval(pg_get_serial_sequence('team', 'id'))")!!.getInt(1)
        connection.createStatement().executeUpdate(
                """
                insert into team values (
                    $teamId, 
                    $$${randomString(TEAM_NAME_LENGTH_RANGE.random())}$$, 
                    ${YEAR_OF_FOUNDATION_RANGE.random()}
                )
                """
        )

        val titlesPerTeam = TITLES_PER_TEAM.random()
        val randomTeamTitles = resultSetToList(randomRow("team_title", titlesPerTeam))
        for (j in 1..titlesPerTeam) {
            connection.createStatement().executeUpdate(
                    "insert into team_titles values (default, $teamId, ${randomTeamTitles.random()})"
            )
        }

        val randomCountries = resultSetToList(randomRow("country", PLAYERS_NUMBER))
        for (j in 1..PLAYERS_NUMBER) {
            val playerId = executeSQL("select nextval(pg_get_serial_sequence('player', 'id'))")!!.getInt(1)
            connection.createStatement().executeUpdate(
                    """
                    insert into player values (
                            $playerId,
                            $$${faker.name().fullName()}$$,
                            ${randomCountries.random()},
                            $$${randomDate(BIRTHDAY_FROM, BIRTHDAY_TO)}$$,
                            $teamId
                    )
                    """
            )

            val titlesPerPlayer = TITLES_PER_PLAYER.random()
            val randomPlayerTitles = resultSetToList(randomRow("player_title", titlesPerPlayer))
            for (k in 1..titlesPerPlayer) {
                connection.createStatement().executeUpdate(
                        "insert into player_titles values (default, $playerId, ${randomPlayerTitles.random()})"
                )
            }
        }
    }

    val seasonIdList = mutableListOf<Int>()
    for (j in 1..SEASONS_COUNT) {
        val seasonId = executeSQL("select nextval(pg_get_serial_sequence('season', 'id'))")!!.getInt(1)
        val matchDateFrom = "${START_YEAR + j - 1}-01-01"
        connection.createStatement().executeUpdate(
                "insert into season values ($seasonId, $$$matchDateFrom$$)"
        )
        seasonIdList += seasonId
    }

    for (i in 1..tablesMap["competition"]!!) {
        val competitionId = executeSQL("select nextval(pg_get_serial_sequence('competition', 'id'))")!!.getInt(1)
        connection.createStatement().executeUpdate(
                "insert into competition values ($competitionId, $$${randomString(COMPETITION_NAME_LENGTH_RANGE.random())}$$)"
        )

        for (j in 1..SEASONS_COUNT) {
            val matchDateFrom = "${START_YEAR + j - 1}-01-01"
            val matchDateTo = "${START_YEAR + j - 1}-12-31"
            val seasonId = seasonIdList[j - 1]

            val randomFirstTeam = resultSetToList(randomRow("team", TEAMS_PER_COMPETITION))
            for (k in 1..TEAMS_PER_COMPETITION) {
                val firstTeamId = randomFirstTeam.random()
                val randomSecondTeam = resultSetToList(randomRow("team", TEAMS_PER_COMPETITION - k))
                for (l in k + 1..TEAMS_PER_COMPETITION) {
                    val matchId = executeSQL("select nextval(pg_get_serial_sequence('match', 'id'))")!!.getInt(1)
                    val secondTeamId = randomSecondTeam.random()
                    connection.createStatement().executeUpdate(
                            """
                        insert into match values (
                            $matchId, 
                            $$${randomDate(matchDateFrom, matchDateTo)}$$, 
                            $firstTeamId, 
                            $secondTeamId, 
                            ${GOALS_SCORED_MATCH_RANGE.random()},
                            ${GOALS_SCORED_MATCH_RANGE.random()},
                            $competitionId,
                            $seasonId
                        )
                        """
                    )

                    val randomPlayers = resultSetToList(randomRow("player", PLAYERS_NUMBER * 2))
                    for (m in 1..PLAYERS_NUMBER * 2) {
                        connection.createStatement().executeUpdate(
                                """
                            insert into match_statistics values  (
                                default,
                                $matchId,
                                ${randomPlayers.random()},
                                ${GOALS_PER_PLAYER_RANGE.random()},
                                ${YELLOW_CARDS_PER_PLAYER_RANGE.random()},
                                ${nextBoolean()},
                                ${DASHES_PER_PLAYER_RANGE.random()},
                                ${SAVES_PER_PLAYER_RANGE.random()},
                                ${TACKLES_PER_PLAYER_RANGE.random()}
                            )
                                """
                        )
                    }
                }
                connection.createStatement().executeUpdate(
                        """
                    insert into standings values (
                            default, 
                            $firstTeamId, 
                            ${POINTS_RANGE.random()},
                            $competitionId,
                            $seasonId
                    )
                    """
                )
            }
        }
    }

    connection.close()
}

fun parseArgs(args: Array<String>) {
    val options = Options().apply {
        var i = 1
        for ((table, _) in defaultSizes) {
            addOption("${i++}", table, true, "size of table \"$table\"")

        }
        addOption("r", "truncate all tables")
    }

    val cmd: CommandLine = try {
        DefaultParser().parse(options, args)
    } catch (e: ParseException) {
        println(e)
        HelpFormatter().printHelp("myscore-data", options)
        exitProcess(1)
    }

    truncate = cmd.hasOption("r")
    val unnamedArgs = cmd.argList
    var i = 0
    for ((table, defaultSize) in defaultSizes) {
        val size = when {
            cmd.hasOption(table) -> cmd.getOptionValue(table)
            i < unnamedArgs.size -> unnamedArgs[i]
            else -> null
        }?.toIntOrNull()

        tablesMap += Pair(table, if (size != null && size > 0) size else defaultSize)
        i++
    }
}

fun connect(): Connection {
    val url = "jdbc:postgresql://localhost:5432/postgres"
    val user = "postgres"
    val password = "postgres"
    DriverManager.registerDriver(Driver())
    return DriverManager.getConnection(url, user, password)
}

fun executeSQL(statement: String): ResultSet? {
    val result = connection.prepareStatement(statement)
    return result.apply { if (!execute()) return null }.resultSet.apply { next() }
}

fun randomRow(table: String, count: Int): ResultSet {
    val result = connection.prepareStatement(
        "select * from $table offset floor(random() * (select count(*) from $table)) limit $count"
    )
    return result.apply { execute() }.resultSet
}

fun resultSetToList(rs: ResultSet): List<Int> {
    val tablesList = mutableListOf<Int>()
    while (rs.next())
    tablesList.add(rs.getInt(1))

    return tablesList
}

fun randomString(length: Int): String {
    val allowedLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ "
    return (0..length).map { allowedLetters.random() }.joinToString("")
}

fun randomDate(from: String, to: String) =
        Date(ThreadLocalRandom.current().nextLong(parser.parse(from).time, parser.parse(to).time))