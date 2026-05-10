package org.datasource.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import java.util.logging.Logger;

//@Configuration @EnableWebSecurity
public class BasicConfiguration {
    private static Logger logger = Logger.getLogger(BasicConfiguration.class.getName());

    @Value( "${spring.security.user.name}")
    private String dataServiceUser;
    @Value( "${spring.security.user.password}")
    private String dataServicePassword;

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
        UserDetails user = User.withUsername(dataServiceUser)
                .password(passwordEncoder.encode(dataServicePassword))
                .authorities("MEMBER")
                .build();

        logger.info(">>> In memory User: " + user);

        return new InMemoryUserDetailsManager(user);
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        logger.info(">>> DEBUG: BasicConfiguration: filterChain ...");
        return http
                // Configure endpoint authorization
                .authorizeHttpRequests((authorize) -> authorize
                        .requestMatchers("/**").hasAuthority("MEMBER")
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults())
                .csrf(csrf -> csrf.disable())
                .build();
    }

    @Bean
    public BCryptPasswordEncoder bCryptPasswordEncoder() {
        return new BCryptPasswordEncoder();
    }
}

// auth.requestMatchers(HttpMethod.POST, "/**").hasRole("ADMIN")