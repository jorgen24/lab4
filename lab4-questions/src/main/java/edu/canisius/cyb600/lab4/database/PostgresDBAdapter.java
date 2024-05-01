package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> getAllDistinctCategoryNames() {
        List<String> categories = new ArrayList<>();
        try {
            String query = "SELECT DISTINCT name FROM category";
            try (PreparedStatement statement = conn.prepareStatement(query);
                 ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    categories.add(resultSet.getString("name"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return categories;
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        List<Film> films = new ArrayList<>();
        try {
            String query = "SELECT * FROM film WHERE length > ?";
            try (PreparedStatement statement = conn.prepareStatement(query)) {
                statement.setInt(1, length);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Film film = new Film();
                        film.setFilmId(resultSet.getInt("FILM_ID"));
                        film.setTitle(resultSet.getString("TITLE"));
                        film.setLength(resultSet.getInt("LENGTH"));
                        films.add(film);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return films;
    }

    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        List<Actor> actors = new ArrayList<>();
        try {
            String query = "SELECT * FROM actor WHERE first_name = ?";
            try (PreparedStatement statement = conn.prepareStatement(query)) {
                statement.setString(1, firstLetter + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Actor actor = new Actor();
                        actor.setActorId(resultSet.getInt("ACTOR_ID"));
                        actor.setFirstName(resultSet.getString("FIRST_NAME"));
                        actor.setLastName(resultSet.getNString("LAST_NAME"));
                        actors.add(actor);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return actors;
    }
}