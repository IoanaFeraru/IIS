package org.spark.service.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Security config:
 *  - GET /rest/**  → open (read-only views — no auth needed for OLAP consumers)
 *  - POST /_sqlrest/** → requires Basic Auth (spark / sql)
 *    This includes the createJSONViewFromREST callback from java_method()
 *  - Spring Security CSRF disabled (stateless REST API)
 */
@Configuration
@EnableWebSecurity
public class BasicSecurityConfiguration {

    @Value("${spring.security.user.name:spark}")
    private String username;

    @Value("${spring.security.user.password:sql}")
    private String password;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf().disable()
                .authorizeRequests()
                // Health check and view reads — open
                .antMatchers("/rest/ping").permitAll()
                .antMatchers("/rest/view/**").permitAll()
                .antMatchers("/rest/STRUCT/**").permitAll()

                // SQL execution and view creation — require auth
                .antMatchers("/_sqlrest/**").authenticated()

                // Everything else — open
                .anyRequest().permitAll()
                .and()
                .httpBasic();

        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        return new InMemoryUserDetailsManager(
            User.withUsername(username)
                .password(encoder.encode(password))
                .roles("ADMIN")
                .build()
        );
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
}
