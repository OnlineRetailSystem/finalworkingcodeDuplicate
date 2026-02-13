package com.ecom.authservice.repositories;

import com.ecom.authservice.models.User;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

//data access interface for the user entities
//JPA repo -- inherits a set of standard CRUD (Create, Read, Update, Delete) operations like save(), findById(), delete(), etc.
//additionally findByUsername method is added
public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByUsername(String username);
}


//why name respository ??
//repository pattern abstracts the data layer, providing a clean API for accessing domain objects