package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthInfo implements Serializable {

    /**
     * The user's id.
     */
    private long authorId;

    /**
     * The password used when login by id.
     */
    private String password;

}
